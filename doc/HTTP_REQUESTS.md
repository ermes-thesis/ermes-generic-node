# HTTP REQUESTS

In this file we store all the HTTP requests present in the system. They are divided between **internal** (i.e. exchanged between Ermes nodes - Central Repository - Function Registry) and **external** (i.e. the client uses them in order to interact with Ermes).

All the requests are performed to an URL formatted as: `http://<ermes_node_ip>/<ermes_request>`. In this file we focus only on the `<ermes_request>` part.

## INTERNAL

All the internal requests have an implicit header `X-ERMES-SECRET` which is automatically set by Ermes.

The body is always serialized to be sent and received as `json`.

### ERMES GENERIC NODE

----------

* `POST create_metadata/<collection_id>`:

*Headers*: 
* `X-ERMES-METADATA-DOMAIN` : stirng

*Body*: (Metadata of the collections)
* `Name` : string
* `Owner` : string
* `CreatedAt` : time
* `Groups` : string[]
* `Consistency` : STRONG : 0 | EVENTUAL : 1
* `Geolocation` : string
* `CustomMetadata` : map\[string\]string

*Return*:
*Created* (201) if ok, else *InternalServerError* (500)

----------

* `GET lookup`:

*Headers*:
* `X-ERMES-QUERY` : string
* `X-ERMES-POSITION` : string
* `X-ERMES-GROUPS` : string

*Body*:

*Return*:
`InternalServerError` if queries failed or if failed to serialize response.

If all ok return in the body:
`map[string]({string, Metadata}[])`

(Basically map to array of objects containing both string -> query alias and metadata of collections associated).

----------

* `GET data-transfer-request`:

*Headers*:

*Body*:

*Return*:
*BadRequest* (400) if body is not formatted well.

----------

* `POST gossip`:

*Headers*:

*Body*:
Depends on the type.
There is the **BaseGossip** :
* `messageType` : string
* `senderId` : string
And the children, **ChildState** : 
* `freeRedisMemory` : float64
**LeaderDelega** : 
* `freeRedisMemory` : float64

*Return*:
*OK* (200) if all good, else *BadRequest* if the body is badly formatted.

----------

* `POST repo`:

*Headers*:

*Body*:
* `CollectionId` : string
* `Records` : map[string]string
* `Groups` : string[]
* `IsStrong` : bool
* `IsLeader` : bool
* `CollectionSize` : uint64

*Return*:
*OK* (200) if all good, else *BadRequest* (400) if the body is badly formatted or the message does not exist.

----------

* `POST dataRemoteAccess`:

*Headers*:

*Body*:
* `collectionId` : string
* `recordNames` : string[]

*Return*:
In the body a json containing `records` : any[] which is an array of redis objects.

*OK* (200) if all good, else *BadRequest* (400) if the body is badly formatted or if there was an issue accessing the internal db.

----------

* `POST dataRemoteUpdate`:

*Headers*:

*Body*:
* `collectionId` : string
* `recordNames` : string[]
* `records` : string[]

*Return*:
*OK* (200) if all good, else *BadRequest* (400) if the body is badly formatted or if there was an issue accessing the internal db.

### ERMES CENTRAL REPO

----------

* `POST group/<group_name>`:

*Headers*:

*Body*:
* `participantIDs` : string[]

*Return*:
*OK* (200) if everything went well, *BadRequest* (400)if the request is not formatted correctly, *InternalServerError* (500) if the body was not formatted correctly or the group could not be added to the db.

----------

* `DELETE group/<group_name>`:

*Headers*:

*Body*:

*Return*:
*OK* (200) if everything went well, *BadRequest* (400) if the request is not formatted correctly, *InternalServerError* (500) if the db instance failed to delete the group.

----------

* `POST group/<group_name>/<user_id>`:

*Headers*:

*Body*:

*Return*:
*OK* (200) if everything went well, *BadRequest* (400) if the request is not formatted correctly, *InternalServerError* (500) if the db instance failed to add the user to the group.

----------

* `DELETE group/<group_name>/<user_id>`:

*Headers*:

*Body*:

*Return*:
*OK* (200) if everything went well, *BadRequest* (400) if the request is not formatted correctly, *InternalServerError* (500) if the db instance failed to remove the user from the group.

### ERMES FUNCTION REGISTRY

----------

* `GET retrieve-function/<name>`:

*Headers*:
* `If-None-Match` : string

*Body*:

*Return*:
*InternalServerError* (500) if the function could not be retrieved or the query was not retrieved/parsed correctly or if the response was not built correctly, *BadRequest* (400) if you try to retrieve an authentication function, *NotModified* (304) if the function cached in the node is the latest.

In the response headers we find the `ETag` field, containing the function version. And in the body we find the `Binary : byte[]`, `Metadata`, `Templates : map[string]Query`.

----------

* `GET retrieve-function/<name>/<version>`:

*Headers*:
* `If-None-Match` : string

*Body*:

*Return*:
*InternalServerError* (500) if the function could not be retrieved or the query was not retrieved/parsed correctly or if the response was not built correctly, *BadRequest* (400) if you try to retrieve an authentication function, *NotModified* (304) if the function cached in the node is the latest.

In the response headers we find the `ETag` field, containing the function version. And in the body we find the `Binary : byte[]`, `Metadata`, `Templates : map[string]Query`.

----------

* `GET retrieve-auth-function/<name>`:

Mirror of `retrieve-function/<name>`.

----------

* `GET retrieve-auth-function/<name>/<version>`:

Mirror of `retrieve-function/<name>/<version>`.

## EXTERNAL

### ERMES GENERIC NODE

----------

* `invoke-function/<function_name>`:
* `invoke-function/<function_name>/<function_version>`:

If the version is not specified, the latest version is invoked.
Any HTTP method can be used (GET, POST, PUT, DELETE, ...), chosen by the user.

**Request**

*Headers*:
* `X-ERMES-QUERY`: map[string]map[string]string (template name -> (placeholder name -> placeholder value))
* `X-ERMES-CACHED-QUERY`: map[string][]model.CollectionInfo (query `AS` field -> array of collection info objects)
* `X-ERMES-SEQUENTIAL-NUMBER`: uint64 (sequence number of the sequential collection)
* `X-ERMES-POSITION`: string (geolocation for metadata lookup)
* `X-ERMES-GROUPS`: string (group token)
* `X-CLIENT-SECRET`: string (secret of the client library)

*Body*:
* `map[string]string` that is the input parameters for the function

**Response**

*Status code*: returned by the function.

*Headers*:
* `X-ERMES-SEQUENTIAL-NUMBER`: uint64 (updated sequence number of the sequential collection)
* `X-ERMES-CACHE`: map[uint64]model.QueryResult (query identifier -> query result object) - contains the results of the queries executed during the function invocation that can be cached for future invocations
* `X-ERMES-EXECUTION-HOST`: string (hostname of the Ermes node that executed the function)

*Body*:
* `any` that is the output of the function

The Client library should persist the cached queries and the sequential number between invocations.
Specifically, when invoking a function that uses a sequential collection, the client library should always provide the latest sequential number received in the response headers of the previous invocation.

The `X-ERMES-CACHE` response header should be merged with the existing cached queries in the client library.
The key of the map is the query identifier, which is computed as the hash of the query template name and the placeholder values used in the invocation.
When invoking a function, the client library should compute the QueryIdentifier for each query requested by the user.
If it is present in the cached queries, it adds the corresponding query result to the `X-ERMES-CACHED-QUERY` request header (please note that the value of this header is a map from the query `AS` field, to the array of collection info objects).
Otherwise, the `X-ERMES-QUERY` header will contain the query template with the placeholder values to be executed by Ermes.

All the types CollectionInfo, QueryResult and QueryIdentifier are defined in the `model` package of the Ermes Common repository.

### ERMES CENTRAL REPO

----------

* `GET token`:

*Headers*:
* `X-CLIENT-SECRET` : string (secret of the client library)
* `Authorization` : string (contains the authentication header required by the authentication function)

*Body*:

*Return*:
*OK* (200) if all went well,
*BadRequest* (400) if the header is not present or badly formatted,
*Unauthorized* (401) if the authentication function failed to execute,
*InternalServerError* (500) if the groups for the user could not be retrieved or the group token failed to generate or the response was not encoded correctly.

If the request was successful in the body a `map[string]string` is returned, containing for the key `group_token` the corresponding token.

----------

* `PUT set-auth-function/<name>`:

*Headers*:
* `X-DEV-SECRET` : string

*Body*:

*Return*:
*OK* (200) if everything went well and the function metadata was updated to the latest version, *NotFound* (404) if the function metadata were not found.

----------

* `PUT set-auth-function/<name>/<version>`:

*Headers*:
* `X-DEV-SECRET` : string

*Body*:

*Return*:
*OK* (200) if everything went well and the function metadata was updated, *NotFound* (404) if the function metadata were not found.

### ERMES FUNCTION REGISTRY

----------

* `POST store-function`:

*Headers*:
* `X-DEV-SECRET` : string
* `X-Overwrite` : bool

*Body*:
* `map[string]byte[]` -> contains also field `metadata`

*Return*:
*OK* (200) if all went well, *BadRequest* (400) if the body or metadata is not formatted well, *InternalServerError* (500) if function could not be stored in the db.

----------

* `POST store-query`:

*Headers*:
* `X-DEV-SECRET` : string
* `X-OVERWRITE` : bool

*Body*:
* `map[string]Query`

*Return*:
*OK* (200) if all went well, *BadRequest* (400) if the body or metadata is not formatted well, *InternalServerError* (500) if the body of at least one query could not be parsed or could not be saved correctly in the db.