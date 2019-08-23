# Medallion TAXII endpoints
DISCOVERY_EP = "/taxii/"
API_ROOT_EP = "/trustgroup1/"  # there are other API roots, but this is the one used in tests

COLLECTIONS_EP = API_ROOT_EP + "collections/"
GET_COLLECTION_EP = COLLECTIONS_EP + "52892447-4d7e-4f70-b94d-d7f22742ff63/"
GET_ADD_COLLECTION_EP = COLLECTIONS_EP + "91a7b528-80eb-42ed-a74d-c6fbd5a26116/"
NON_EXISTENT_COLLECTION_EP = COLLECTIONS_EP + "12345678-1234-1234-1234-123456789012/"
FORBIDDEN_COLLECTION_EP = COLLECTIONS_EP + "64993447-4d7e-4f70-b94d-d7f33742ee63/"
EMPTY_COLLECTION_EP = COLLECTIONS_EP + "472c94ae-3113-4e3e-a4dd-a9f4ac7471d4/"

MANIFESTS_EP = GET_ADD_COLLECTION_EP + "manifest/"
OBJECTS_EP = GET_ADD_COLLECTION_EP + "objects/"
GET_OBJECT_EP = GET_OBJECTS_EP = ADD_OBJECTS_EP = OBJECTS_EP

GET_COLLECTION_EP_FOR_DIRECTORY_BACKEND = COLLECTIONS_EP + "46bb17fa-0af3-3446-a570-b55cdfdc7881/"
GET_OBJECTS_FROM_DIRECTORY_BACKEND_EP_NOT_EXISTANT = COLLECTIONS_EP + "indicator--d772d620-9720-4457-897d-7030d1a972fd/"
GET_OBJECTS_FROM_DIRECTORY_BACKEND_EP = GET_COLLECTION_EP_FOR_DIRECTORY_BACKEND + "objects/"
GET_OBJECTS_FROM_DIRECTORY_BACKEND_EP_NOT_EXISTANT = \
    COLLECTIONS_EP + "64bb17fa-0af3-3446-a570-b55cdfdc7818/" + "objects/"
ADD_OBJECTS_FOR_DIRECTORY_BACKEND_EP = GET_OBJECTS_FROM_DIRECTORY_BACKEND_EP

LOGIN = "/login"
