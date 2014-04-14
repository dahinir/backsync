backsync
========

A *minimalistic* library for integrating Backbone models with different data stores. Mongodb, Couchdb and Memory already included.

# Installation

```bash
$ npm install backsync
```

# Usage

```javacript
var backsync = require( "backsync" );

var Todo = Backbone.Model.extend({
    urlRoot: "mongodb://127.0.0.1:27017/test/todos",
    sync: backsync.mongodb(); // alternatively, use backsync.memory()
})
```

> You can also override the general Backbone.sync method in order to affect all of the Models in the application, instead of the specific Models and Collections.

Then, you can create, update, delete and read all of the model instances normally following Backbone's documentation. The same applies for Collections, and run queries with the `.fetch()` command:

```javacript
var TodoCollection = Backbone.Model.extend({
    url: "mongodb://127.0.0.1:27017/test/todos",
    sync: backsync.mongodb(); // alternatively, use backsync.memory()
});

new TodoCollection()
    .on( "sync", function() {
        console.log( this.models );
    }).fetch({
        data: {
            user: "123",
            $sort: "order",
            $skip: 10,
            $limit: 10
        }
    });

```

### Building your own backend

In order to create your own data stores for Backbone, implement the following methods: `create` (alias: `insert`), `update`, `read`, `delete` (alias: `remove`) and `search` (alias: `list`). Each of these functions receive the model, options object and a callback to call with the updated resource attributes.

See the implementation of the memory or mongo sync methods to learn more.
