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

* ``:

*Headers*:

*Body*:

*Return*:

### ERMES CENTRAL REPO

----------

* `token`:

*Headers*:
* `X-MIDDLEWARE-SECRET` : string
* `Authorization` : string

*Body*:

*Return*:
*OK* (200) if all went well, *NoAuthHeaderErr* or *InvalidAuthHeaderErr* if the header is not present or badly formatted, *AuthFailedErr* if the authentication function failed to execute, *InternalServerError* (500) if the groups for the user could not be retrieved or the group token failed to generate or the response was not encoded correctly.

If the request was succesful in the body a `map[string]string` is returned, containing for the key `group_token` the corresponding token.

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