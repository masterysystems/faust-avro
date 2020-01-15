"""
A simple event-sourced user service
"""

import datetime
import functools
import logging
import typing

import aiohttp.web
import faust
import strawberry
import strawberry.asgi.http
import strawberry.asgi.utils
import strawberry.graphql
import strawberry.types.datetime

from faust_avro import App, Record


class UserExistsError(Exception):
    pass


class UserDoesNotExistError(Exception):
    pass


##############################################################################
# Models
##############################################################################
class User(Record, coerce=True):
    """The main data model for a user, stored in our table."""

    email: str
    name: str
    joined: datetime.datetime

    def __str__(self):
        return f"{name} <{email}>"


class UserKey(Record):
    key: str


class UserCreated(Record, coerce=True):
    user: User


class NameChanged(Record, coerce=True):
    email: str
    name: str


class UpdatedEmail(Record, coerce=True):
    old_email: str
    new_email: str


class UserDeleted(Record, coerce=True):
    email: str


class UserRequest(Record, coerce=True):
    update: typing.Union[UserCreated, NameChanged, UpdatedEmail, UserDeleted]


##############################################################################
# App
##############################################################################
app = App(
    "users", broker="kafka://localhost", reply_create_topic=True, topic_partitions=1
)
users_requests = app.topic(
    "_users_requests", key_type=UserKey, value_type=UserRequest, internal=True
)
cleaned_users_requests = app.topic(
    "users", key_type=UserKey, value_type=UserRequest, internal=True
)
users_table = app.Table("users_table", partitions=1)


##############################################################################
# Business logic
##############################################################################
@functools.singledispatch
async def update_handler(msg: typing.Any):
    raise NotImplementedError(f"No handler for {msg}")


@update_handler.register
async def user_created(msg: UserCreated):
    email = msg.user.email
    if email in users_table:
        raise UserExistsError(f"User with {email} already exists.")
    users_table[email] = msg.user


@update_handler.register
async def name_changed(msg: NameChanged):
    user = users_table[msg.email]
    user.name = msg.name
    users_table[msg.email] = user


@update_handler.register
async def updated_email(msg: UpdatedEmail):
    if msg.old_email == msg.new_email:
        pass
    if msg.old_email not in users_table:
        raise UserDoesNotExistError(f"User with {msg.old_email} does not exist.")
    if msg.new_email in users_table:
        raise UserExistsError(f"User with {msg.new_email} already exists.")
    user = users_table[msg.old_email]
    user.email = msg.new_email
    users_table[msg.new_email] = user
    # This is subtle. We jump from the agent for partition new_email over to
    # the agent for partition old_email and request a delete there. For a
    # short time, the user will exist under both email addresses.
    await users_requests.send(
        key=UserKey(msg.old_email), value=UserRequest(UserDeleted(msg.old_email))
    )


@update_handler.register
async def deleted_email(msg: UserDeleted):
    if msg.email not in users_table:
        raise UserDoesNotExistError(f"User with {msg.email} does not exist.")
    del users_table[msg.email]


##############################################################################
# Agent
##############################################################################
@app.agent(users_requests)
async def users_svc(requests):
    async for key, value in requests.items():
        try:
            await update_handler(value.update)
            await cleaned_users_requests.send(key=key, value=value)
            yield 200  # OK
        except UserExistsError:
            yield 409  # Conflict
        except UserDoesNotExistError:
            yield 404  # Not Found
        except NotImplementedError as e:
            logging.error(e)
            yield 501  # Not Implemented
        except Exception as e:
            logging.error(e)
            yield 500  # Internal Server Error


@app.agent(cleaned_users_requests)
async def cleaned_users_requests(requests):
    async for value in requests:
        # Silly, but faust-avro uses the agent to do topic-schema registration
        pass


##############################################################################
# RESTish
##############################################################################
@app.page("/users")
class users(faust.web.View):
    async def get(self, request: faust.web.Request) -> faust.web.Response:
        """List all users"""
        return self.json(dict(users=dict(users_table.items())))

    async def post(self, request: faust.web.Request) -> faust.web.Response:
        """Create a new user"""
        data = await request.json()
        key = UserKey(data["email"])
        user = User(**data, joined=datetime.datetime.now())
        value = UserRequest(UserCreated(user))
        response = await users_svc.ask(key=key, value=value)
        if response == 200:
            return self.json(dict(user=user.asdict()))
        elif response == 409:
            raise aiohttp.web.HTTPConflict()
        else:
            raise aiohttp.web.HTTPInternalServerError()


@app.page("/users/{email}")
class users_update(faust.web.View):
    @app.table_route(table=users_table, match_info="email")
    async def get(
        self, request: faust.web.Request, *, email: str
    ) -> faust.web.Response:
        """Get a specific user"""
        try:
            return self.json(dict(user=users_table[email].asdict()))
        except KeyError:
            raise aiohttp.web.HTTPNotFound()

    @app.table_route(table=users_table, match_info="email")
    async def patch(
        self, request: faust.web.Request, *, email: str = None
    ) -> faust.web.Response:
        """Update a specific user"""
        data = await request.json()
        if "name" in data:
            update = NameChanged(email, data["name"])
        elif "new_email" in data:
            update = UpdatedEmail(email, data["new_email"])
            # Note this re-routes what partition we'll send on
            email = data["new_email"]
        else:
            raise aiohttp.web.HTTPBadRequest()
        response = await users_svc.ask(key=UserKey(email), value=UserRequest(update))
        if response == 200:
            return self.json(dict(user=users_table[email].asdict()))
        elif response == 404:
            raise aiohttp.web.HTTPNotFound()
        elif response == 409:
            raise aiohttp.web.HTTPConflict()
        else:
            raise aiohttp.web.HTTPInternalServerError()


##############################################################################
# GraphQLish
##############################################################################
@strawberry.type
class UserType:
    email: str
    name: str
    joined: strawberry.types.datetime.DateTime


@strawberry.type
class Query:
    @strawberry.field
    def users(self, info, email: str = None) -> typing.List[UserType]:
        if email is not None:
            return [users_table[email]]
        else:
            return list(users_table.values())


@strawberry.input
class CreateUserInput:
    email: str
    name: str


@strawberry.input
class ChangeUserNameInput:
    email: str
    name: str


@strawberry.input
class ChangeUserEmailInput:
    old_email: str
    new_email: str


@strawberry.type
class Mutation:
    @staticmethod
    async def ask(email, message):
        response = await users_svc.ask(key=UserKey(email), value=UserRequest(message))
        if response == 200:
            return
        else:
            raise Exception("Failure")

    @strawberry.mutation
    async def create_user(self, info, input: CreateUserInput) -> UserType:
        user = User(email=input.email, name=input.name, joined=datetime.datetime.now())
        await Mutation.ask(input.email, UserCreated(user))
        return user

    @strawberry.mutation
    async def change_user_name(self, info, input: ChangeUserNameInput) -> UserType:
        await Mutation.ask(input.email, NameChanged(input.email, input.name))
        return users_table[input.email]

    @strawberry.mutation
    async def change_user_email(self, info, input: ChangeUserEmailInput) -> UserType:
        await Mutation.ask(
            input.new_email, UpdatedEmail(input.old_email, input.new_email)
        )
        return users_table[input.new_email]


schema = strawberry.Schema(query=Query, mutation=Mutation)


# TODO -- routing! Currently this abuses partitions=1 and workers=1 to have consistency.
#
# Routing is a lot harder in graphql. It potentially needs to happen at the mutation level?
# It'd be worth investigating if the response could be the user object itself and/or an
# exception object. Serializing them with pickle would be okay since it is python/faust
# internal and not intended for outside consumption.
@app.page("/graphql")
class graphql(faust.web.View):
    async def get(self, request: faust.web.Request) -> faust.web.Response:
        html = strawberry.asgi.utils.get_playground_html(
            "http://localhost:6066/graphql"
        )
        return aiohttp.web.Response(body=html, content_type="text/html")

    async def execute(self, query, variables=None, context=None, operation_name=None):
        return await strawberry.graphql.execute(
            schema,
            query,
            variable_values=variables,
            operation_name=operation_name,
            context_value=context,
        )

    async def post(self, request: faust.web.Request) -> faust.web.Response:
        response = await strawberry.asgi.http.get_http_response(request, self.execute)
        return aiohttp.web.Response(
            body=response.body, content_type=response.media_type
        )
