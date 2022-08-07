# Guidelines

## Requirements
Use the file `data.csv` as the data source.

Implement the following APIs, using memory only - no external DB.
<br>
**Keep efficiency in mind, the user base (data.csv) can grow exponentially
<br>
Searching the data linearly is not a good enough solution**
```
Get user by Id
    - GET /users/a2ee2667-c2dd-52a7-b9d8-1f31c3ca4eae
    - Should return the requested user details 

Example required response:
{
    "id": "ae8da2bf-69f6-5f40-a5e6-2f1fedb5cea6",
    "name": "Ricardo Wise",
    "dob": "13/1/1973",
    "country": "AE"
}

Get users list by country
    - GET /users?country=US
    - Should return a list of all users from requested country

Get users list by age
    - GET /users?age=30
    - Should return all users which are of age 30 at the time of the request

Get users list by name
    - GET /users?name=Susan
    - Should return all users which name matches the requested name
    - Matching names rules:
        - Full match - for input "Susan James" should return all users with name "Susan James".
        - Full first name or last name - for input "Susan" should return all users with that first or last name.
        - Partial match (minimum 3 chars) - for input "Sus", should return all users with first or last name that begin with "Sus".
        - Should support non case sensitive search (Searching for "susan" should return users with name "Susan").

Example required response for list of users:
[    
    {
        "id": "ae8da2bf-69f6-5f40-a5e6-2f1fedb5cea6",
        "name": "Ricardo Wise",
        "dob": "13/1/1973",
        "country": "AE"
    }
]

Delete user by id
    - DELETE /users/a2ee2667-c2dd-52a7-b9d8-1f31c3ca4eae
    - Should delete the user, after the call the user will not be returned by any of the previous APIs.
```

You can either take the `data.csv` file and create a new project in your preferred language/environment, or use the attached boilerplate written in node.js and implement only the logic part.

## Using boilerplate

### Use this project directory as a template
In order to use this project as a template, please implement the missing methods in the `model/users.js` file.

### Install node
Follow the instructions here:
https://nodejs.org/en/download/

### Start up the service
```
npm install
node index.js
```