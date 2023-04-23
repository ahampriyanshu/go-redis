## In-memory key value database

### Base URL

- [https://greedy-games.onrender.com](https://greedy-games.onrender.com)

#### Saving a value with any conditions

##### Request

```
POST /

{"command": "SET a 5" }
```

##### Response

```
200 OK

{

 }
```

Retrieving the value

```
POST /

{"command": "GET a" }
```

##### Response

```
200 OK

{
  "value": "5"
}
```

Retrieving a non-existing value

```
POST /

{"command": "GET abc" }
```

##### Response

```
400 Bad Request

{
  "error": "key does not exist"
}
```

#### Saving a value with expiry time

##### Request

```
POST /

{"command": "SET a 5 EX 20" }
```

##### Response

```
200 OK

{ }

```

Retrieving the value before expiration

```
POST /

{"command": "GET a" }
```

##### Response

```
200 OK

{
  "value": "5"
}
```

Retrieving the value after expiration

```
POST /

{"command": "GET a" }
```

##### Response

```
400 Bad Request

{
  "error": "key does not exist"
}
```

#### Saving a value with conditions

Only set the key if it does not already exist.

##### Request

```
POST /

{"command": "SET a 5 NX" }
```

- When key doesn't exists

##### Response

```
200 OK

{

 }
```

- When key does exists

##### Response

```
400 Bad Request

{
  "error": "key already exist"
}
```

Only set the key if it already exists.

##### Request

```
POST /

{"command": "SET a 5 XX" }
```

- When key does exists

##### Response

```
200 OK

{

 }
```

- When key doesn't exists

##### Response

```
400 Bad Request

{
  "error": "key does not exist"
}
```

#### Push

##### Request

```
POST /

{"command": "QPUSH li 1 2 3" }
```

- When a new queue is created

##### Response

```
200 OK

{
  "value": "OK"
}
```

- Appends to the existing value

##### Response

```
200 OK

{
  "value": "OK"
}
```

#### Pop

##### Request

```
POST /

{"command": "QPOP li" }
```

- When the queue is non-empty

##### Response

```
200 OK

{
  "value": "3"
}
```

- When the queue is empty

##### Response

```
400 Bad Request

{
  "error": "queue is empty"
}
```

- When the queue doesn't exists

##### Response

```
400 Bad Request

{
  "error": "queue is empty"
}
```

### Error Handling

- Incorrect HTTP method/Endpoint

```
GET /

{"command": "GET a" }
```

##### Response

```
405 Method Not Allowed

{
  "error": "invalid request"
}
```

- POST request without body

```
POST /


```

##### Response

```
Status: 422 Unprocessable Entity

{
  "error": "invalid request"
}
```

- Invalid command

```
POST /

{"command": "SET a"}
```

##### Response

```
400 Bad Request

{
  "error": "invalid command"
}
```
