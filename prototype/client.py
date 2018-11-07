from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from typing import Any, Iterable, Optional, Dict, Union, Tuple

from azure.cosmos.cosmos_client import CosmosClient as _CosmosClient
from azure.cosmos.errors import HTTPFailure


class UsersManagementMixin:
    def create_user(self, user):  # TODO: upsert + create or create(fail_if_exists)?
        pass

    def upsert_user(self, user):
        pass

    def get_user(self, resource_id):
        pass

    def list_users(self):
        pass

    def delete_user(self):
        pass


class StoredProceduresMixin:
    def list_stored_procedures(self, query):
        pass

    def get_stored_procedure(self, resource_id):
        pass

    def create_stored_procedure(self):
        pass

    def upsert_stored_procedure(self, trigger):
        pass

    def delete_stored_procedure(self):
        pass


class TriggersMixin:
    def list_triggers(self, query):
        pass

    def get_trigger(self, resource_id):
        pass

    def create_trigger(self):
        pass

    def upsert_trigger(self, trigger):
        pass

    def delete_trigger(self):
        pass


class UserDefinedFunctionsMixin:
    def list_user_defined_functions(self, query):
        pass

    def get_user_defined_function(self, resource_id):
        pass

    def create_user_defined_function(self):
        pass

    def upsert_user_defined_function(self, trigger):
        pass

    def delete_user_defined_function(self):
        pass


class ContainersManagementMixin:
    """
    Manage (create/list/query/get/delete) containers. 
    [Design note] Currently isolated into a mixin class to make it easier to move the functionality around
                  in the object model. 
    """

    def create_container(self, resource_id, options=None, **kwargs) -> "Container":
        """
        :param str resource_id: Id of container to create
        Keyword arguments:
        partitionKey, indexingPolicy, defaultTtl, conflictResolutionPolicy
        """
        definition = dict(id=resource_id)
        definition.update(kwargs)
        database = cast("Database", self)
        data = database._context.CreateContainer(
            database_link=database.database_link, collection=definition, options=options
        )
        return Container(database, data["id"])

    def delete_container(self, container_or_id):
        database = cast("Database", self)
        if isinstance(container_or_id, str):
            container = Container(database, container_or_id)
        else:
            container = container_or_id
        database._context.DeleteContainer(container.collection_link)

    def get_container(self, resource_id):
        containers = self.list_containers(
            query=dict(
                query="SELECT * FROM root r WHERE r.id = @container",
                parameters=[{"name": "@container", "value": resource_id}],
            )
        )
        try:
            return next(containers)
        except StopIteration:
            raise ValueError()

    def list_containers(self, query=None) -> "Iterable[Container]":
        database = cast("Database", self)

        if query:
            yield from [
                Container(database, container["id"])
                for container in database._context.ReadContainers(
                    database_link=database.database_link
                )
            ]
        else:
            yield from [
                Container(database, container["id"])
                for container in database._context.ReadContainers(
                    database_link=database.database_link
                )
            ]


class Client:
    """
    CosmosDB SQL Client. This is the main entry point to the Cosmos DB object model.
    """

    def __init__(self, url, key):
        self._context = _CosmosClient(url, dict(masterKey=key))

    def create_database(self, resource_id: "str", fail_if_exists=False) -> "Database":
        """
        Create a new database
        :param str resource_id: Id of the database to crate.
        :param bool fail_if_exists: If set to True and a database with the given `resource_id` 
        already exists, fail raise a `HTTPFailure`/status code 409. If a database with
        the given id exists, and `fail_if_exists` is False, return the existing database.
        """
        try:
            result = self._context.CreateDatabase(database=dict(id=resource_id))
            return Database(self, resource_id=result["id"])
        except HTTPFailure as e:
            if fail_if_exists and e.status_code == 409:
                raise
        return self.get_database(resource_id)

    def get_database(self, resource_id: "str", metadata_handler=None) -> "Database":
        """
        Return the existing database with the id `resource_id. 
        :param str resource_id: Id of the new database.
        :param callable metadata_handler: Optional method that will, if provided, receive an instance
        of `CosmosCallMetadata` representing metadata about the operation (RSU cost etc.)
        """
        database = Database(client=self, resource_id=resource_id)
        if metadata_handler is not None:
            self._context.ReadDatabase(database.database_link)
            # TODO: Parse last response headers into strongly typed object
            metadata_handler(self._context.last_response_headers)
        return database

    def list_databases(self, query: "Optional[str]" = None) -> "Iterable[Database]":
        """
        Return an iterable of all existing databases.
        :param str query: Cosmos DB SQL query. If omitted, all databases will be listed.
        """
        if query:
            yield from [
                Database(self, database["id"])
                for database in self._context.QueryDatabases(query)
            ]
        else:
            yield from [
                Database(self, database["id"])
                for database in self._context.ReadDatabases()
            ]

    def delete_database(self, resource_id: "str"):
        """
        Delete the database with the given id. Raises a HTTPError if 
        the delete fails. 
        :param str resource_id: The database to delete. 
        """
        self._context.DeleteDatabase(database_link="dbs/" + resource_id)


class Database(ContainersManagementMixin, UsersManagementMixin):
    """
    Azure Cosmos DB SQL Database
    """

    def __init__(self, client: "Client", resource_id: "str"):
        """
        :param Client client: Client from which this database was retreived. TODO: should we hide the client? Should it just be context?
        :param str resource_id: Id of the database
        """
        self.client = client
        self._context = client._context
        self.resource_id = resource_id
        self.database_link = f"/dbs/{self.resource_id}"


class Item(dict):
    def __init__(self, container: "Container", data: "Dict[str, Any]"):
        super().__init__()
        self.container = (
            container
        )  # TODO: Item instances (locally) probably shouldn't be directly tied to a collection
        self._context = container._context
        self.update(data)


class Container(UserDefinedFunctionsMixin, TriggersMixin, StoredProceduresMixin):
    def __init__(self, database: "Database", resource_id: "str"):
        self.database = database
        self._context = database._context
        self.session_token = None
        self.resource_id = resource_id
        self.collection_link = f"{database.database_link}/colls/{self.resource_id}"

    def set_properties(
        self,
        *,
        resource_id=None,
        partition_key=None,
        indexing_policy=None,
        default_ttl=None,
        conflict_resolution_policy=None,
    ):
        """
        Update the properties of the container. Change will be persisted immediately.
        TODO: Should this be on the database class?
        """
        parameters = {
            key: value
            for key, value in {
                "id": resource_id or self.resource_id,
                "partitionKey": partition_key,
                "indexingPolicy": indexing_policy,
                "defaultTtl": int(default_ttl),
                "conflictResolutionPolicy": conflict_resolution_policy,
            }.items()
            # TODO: Questionable use - should use kwargs instead. Need to figure out best documentation for kwargs...
            if value is not None
        }
        self._context.ReplaceContainer(self.collection_link, collection=parameters)

    def get_properties(self, force_refresh=False):
        """
        Get properties for of container. TODO: implement properly
        """
        pass

    @staticmethod
    def _document_link(item_or_link) -> "str":
        if isinstance(item_or_link, str):
            return item_or_link
        return cast("str", cast("Item", item_or_link)["_self"])

    def get_item(self, resource_id: "str", metadata_handler=None, cls=Item) -> "Item":
        """
        Get the item resource_identified by `resource_id`
        :param str resource_id: Id of item to retreive
        :param callable metadata_handler: Optional method that will, if provided, receive an instance
        of `CosmosCallMetadata` representing metadata about the operation (RSU cost etc.)
        :returns: Item if present. TODO: General guidelines is to throw rather than returning None. 
        """
        doc_link = f"{self.collection_link}/docs/{resource_id}"
        result = self._context.ReadItem(document_link=doc_link)
        headers = self._context.last_response_headers
        if metadata_handler:  # TODO: Create strongly typed object for options
            metadata_handler(headers)
        self.session_token = headers.get("x-ms-session-token", self.session_token)
        return cls(container=self, data=result)

    def list_items(self, options=None, cls=Item) -> "Iterable[Item]":
        options = options or {}
        items = self._context.ReadItems(
            collection_link=self.collection_link, feed_options=options
        )
        yield from [cls(self, item) for item in items]

    def query_items(self, query: "str", cls=Item):
        items = self._context.QueryItems(
            database_or_Container_link=self.collection_link, query=query
        )
        yield from [cls(self, item) for item in items]

    def replace_item(self, item: "Union[Item, str]", body: "Dict[str, Any]") -> "Item":
        item_link = Container._document_link(item)
        data = self._context.ReplaceItem(document_link=item_link, new_document=body)
        return Item(self, data)

    def upsert_item(self, body: "Dict[str, Any]") -> "Item":
        result = self._context.UpsertItem(
            database_or_Container_link=self.collection_link, document=body
        )
        return Item(self, result)

    def create_item(self, body: "Dict[str,str]") -> "Item":
        result = self._context.CreateItem(
            database_or_Container_link=self.collection_link, document=body
        )
        return Item(self, result)

    def delete_item(self, item: "Item"):
        document_link = Container._document_link(item)
        self._context.DeleteItem(document_link=document_link)
