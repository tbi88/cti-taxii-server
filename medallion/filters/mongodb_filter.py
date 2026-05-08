from bson.son import SON
from pymongo import ASCENDING

from ..common import datetime_to_float, string_to_datetime
from .basic_filter import BasicFilter


class MongoDBFilter(BasicFilter):

    def __init__(self, filter_args, basic_filter, allowed, record=None):
        super(MongoDBFilter, self).__init__(filter_args)
        self.basic_filter = basic_filter
        self.full_query = self._query_parameters(allowed)
        self.record = record

    def _query_parameters(self, allowed):
        parameters = self.basic_filter
        if self.filter_args:
            match_type = self.filter_args.get("match[type]")
            if match_type and "type" in allowed:
                types_ = match_type.split(",")
                if len(types_) == 1:
                    parameters["type"] = {"$eq": types_[0]}
                else:
                    parameters["type"] = {"$in": types_}
            match_id = self.filter_args.get("match[id]")
            if match_id and "id" in allowed:
                ids_ = match_id.split(",")
                if len(ids_) == 1:
                    parameters["id"] = {"$eq": ids_[0]}
                else:
                    parameters["id"] = {"$in": ids_}
            match_spec_version = self.filter_args.get("match[spec_version]")
            if match_spec_version and "spec_version" in allowed:
                spec_versions = match_spec_version.split(",")
                media_fmt = "application/stix+json;version={}"
                if len(spec_versions) == 1:
                    parameters["_manifest.media_type"] = {
                        "$eq": media_fmt.format(spec_versions[0])
                    }
                else:
                    parameters["_manifest.media_type"] = {
                        "$in": [media_fmt.format(x) for x in spec_versions]
                    }
            added_after_date = self.filter_args.get("added_after")
            if added_after_date:
                added_after_timestamp = datetime_to_float(string_to_datetime(added_after_date))
                parameters["_manifest.date_added"] = {
                    "$gt": added_after_timestamp,
                }
        return parameters

    def process_filter(self, data, allowed, manifest_info):
        pipeline = [
            {"$match": {"$and": [self.full_query]}},
        ]
        latest_pipeline = list()
        # when no filter is provided only latest is considered.
        match_spec_version = self.filter_args.get("match[spec_version]")
        if not match_spec_version and "spec_version" in allowed:
            spec_versions = data.distinct("_manifest.media_type")
            spec_versions.sort()
            pipeline = [
                {
                    "$match": {
                        "$and": [
                            self.full_query,
                            {"_manifest.media_type": spec_versions[-1]},
                        ]
                    }
                }
            ]
        # create version filter
        if "version" in allowed:
            match_version = self.filter_args.get("match[version]")
            if not match_version:
                match_version = "last"
            if "all" not in match_version:
                actual_dates = [datetime_to_float(string_to_datetime(x)) for x in match_version.split(",") if (x != "first" and x != "last")]

                latest_pipeline = []
                if "last" in match_version:
                    # Use the pre-computed _is_latest flag for an O(log n) index lookup
                    # instead of an O(n) $setWindowFields scan over all matching docs.
                    pipeline[0]["$match"]["$and"].append({"_is_latest": True})
                elif "first" in match_version:
                    # "first" is rare; fall back to window function with $min.
                    latest_pipeline.append(
                        {
                            "$setWindowFields": {
                                "partitionBy": "$id",
                                "output": {
                                    "_picked_version": {
                                        "$min": "$_manifest.version"
                                    }
                                }
                            }
                        })
                    latest_pipeline.append({
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$_manifest.version",
                                        "$_picked_version"
                                    ]
                                }
                            }
                        })
                    latest_pipeline.append(
                        {
                            "$unset": [
                                "_picked_version"
                            ]
                        })
                if actual_dates:
                    latest_pipeline.append({"$match": {"_manifest.version": {"$in": actual_dates}}})

            for obj in latest_pipeline:
                pipeline.append(obj)

        pipeline.append({"$sort": SON([("_manifest.date_added", ASCENDING), ("created", ASCENDING), ("modified", ASCENDING)])})

        if manifest_info == "manifests":
            pipeline.append({"$project": {"_manifest": 1}})
            pipeline.append({"$replaceRoot": {"newRoot": "$_manifest"}})
        elif manifest_info == "objects":
            pipeline.append(
                {"$project": {"_id": 0, "_collection_id": 0, "_manifest": 0, "_is_latest": 0}}
            )

        self.add_pagination_operations(pipeline)
        results = list(data.aggregate(pipeline, allowDiskUse=True))

        # Determine whether more pages exist by having fetched limit+1 results.
        # Trim the sentinel document so callers always receive at most limit items.
        limit = self.record.get("limit") if self.record else None
        if limit and len(results) > limit:
            has_more = True
            results = results[:limit]
        else:
            has_more = False

        return has_more, results

    def add_pagination_operations(self, pipeline):
        if self.record and "limit" in self.record:
            pipeline.append({"$skip": self.record["skip"]})
            # Fetch one extra document so we can detect whether another page exists
            # without running a separate count aggregation.
            pipeline.append({"$limit": self.record["limit"] + 1})

