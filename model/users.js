const csv = require("csv-parser");
const fs = require("fs");
var HashMap = require("hashmap");
const { resourceLimits } = require("worker_threads");
const NodeCache = require("node-cache");
const { table } = require("console");

const myCache = new NodeCache({ stdTTL: 100 });
var usersTable = new HashMap();
var agesTable = new HashMap();
var countrysTable = new HashMap();
var namesTable = new HashMap();
let usersToDelete = [];

function insertDataById(table, key, id) {
  if (table.has(key)) {
    if (!table.get(key).includes(id)) {
      table.get(key).push(id);
    }
  }
  else {
    table.set(key, [id]);
  }
}

function calculateAge(dob) {
  var diffMs = Date.now() - dob.getTime();
  var ageDt = new Date(diffMs);

  return Math.abs(ageDt.getUTCFullYear() - 1970);
}
function GetUsersDateFromCsv() {

  fs.createReadStream("data.csv")
    .pipe(csv({}))
    .on("data", (data) => {
      //----set the main table----
      usersTable.set(data.Id, data);
      //----set the age table----
      const [day, month, year] = data.DOB.split("/");
      let converDataToAge = calculateAge(new Date(+year, +month - 1, +day));
      insertDataById(agesTable, converDataToAge, data.Id);
      //----set the country table----
      insertDataById(countrysTable, data.Country, data.Id);
      //----set the name table----
      let name = data.Name.toLowerCase();
      let firstName = name.substring(0, 3);
      let lastName = name.substr(name.indexOf(" ") + 1).substring(0, 3);
      // fix remove this line 
      insertDataById(namesTable, lastName, data.Id);
      insertDataById(namesTable, firstName, data.Id);
    });
}
function usersIds(table, data) {

  if (myCache.has(data)) {
    myCache.ttl(data, 100);
    console.log("cache");
    return myCache.get(data);
  }
  else if (table.has(data)) {
    myCache.set(data, table.get(data));
    console.log("table");
    return table.get(data);
  }
  return -1;
}


module.exports = {
  getUserById: async function (id) {
    console.log(`getUserById called with id: ${id}`);

    if (myCache.has(id)) {
      myCache.ttl(id, 100);
      return myCache.get(id);
    }
    else if (usersTable.has(id)) {
      myCache.set(id, usersTable.get(id));
      return usersTable.get(id);
    }
    return `No user exists with id: ${id}`;
  },

  getUsersByAge: async function (age) {
    let usersRes = [];
    console.log(`getUsersByAge called with age: ${age}`);
    // --- Checking age validation ---
    if (parseInt(age) < 0 || parseInt(age) > 121 || isNaN(age)) {
      return `Invalid value : ${age}`;
    }

    let usersAgeIds = usersIds(agesTable, parseInt(age));

    if (usersAgeIds == -1) {
      return `No user exists with the age: ${age}`;
    }

    for (const user of usersAgeIds) {
      usersRes.push(usersTable.get(user));
    }
    return usersRes;
  },

  getUsersByCountry: async function (country) {
    let usersRes = [];
    console.log(`getUsersByCountry called with country: ${country}`);
    // --- Checking country validation ---
    const validInput = /^[A-Z][A-Z]$/;
    if (validInput.test(country) === false) {
      return `Invalid Input: ${country}`;
    }

    let usersCountryIds = usersIds(countrysTable, country);

    if (usersCountryIds == -1) {
      return `No user exists in the country: ${country}`;
    }

    for (const user of usersCountryIds) {
      usersRes.push(usersTable.get(user));
    }
    return usersRes;
  },

  getUsersByName: async function (name) {
    // --- Checking name validation ---
    const validInput = /^([a-zA-Z])+\s*([a-zA-Z])*$/;
    if (validInput.test(name) === false) {
      return `Invalid Input: ${name}`;
    }

    const re = new RegExp(`\\b${name.toLowerCase()}[^\\s|\\b]*`);
    let usersRes = [];
    let firstTreeNameLetters = name.toLowerCase().substring(0, 3);

    console.log(`searchUsersByName called with name: ${name}`);

    // Add implementation here
    let usersNameIds = usersIds(namesTable, firstTreeNameLetters);
    if (usersNameIds == -1) {
      return `No such user exists: ${name}`;
    }

    for (const user of usersNameIds) {
      let userName = usersTable.get(user).Name.toLowerCase();
      const ifMatch = userName.match(re);
      if (ifMatch != null) {
        usersRes.push(usersTable.get(user));
      }
    }
    if (usersRes.length === 0) {
      return `No such user exists: ${name}`;
    }
    return usersRes;
  },

  deleteUser: async function (id) {
    console.log(`Delete user called with id: ${id}`);
    // Add implementation here
    if (!usersTable.has(id)) {
      return `No user exists with id: ${id}`;
    }
    usersToDelete.push(usersTable.get(id));
    usersTable.delete(id);
  },
};

function removeIdFromList(table, key, id) {
  if (table.has(key)) {
    usersIds = table.get(key);
    usersIds.splice(usersIds.indexOf(id), 1);
  }
  if (myCache.has(key)) {
    myCache.delete(data);
  }

}

function intervalFunc() {
  if (usersToDelete.length !== 0) {
    console.log("user delete!");
    for (const index in usersToDelete) {
      user = usersToDelete[index];
      const [day, month, year] = user.DOB.split("/");
      let converDataToAge = calculateAge(new Date(+year, +month - 1, +day));
      let name = user.Name.toLowerCase();
      let firstName = name.substring(0, 3);
      let lastName = name.substr(name.indexOf(" ") + 1).substring(0, 3);

      removeIdFromList(agesTable, converDataToAge, user.Id);
      removeIdFromList(countrysTable, user.Country, user.Id);
      if (firstName !== lastName) {
        removeIdFromList(namesTable, lastName, user.Id);
      }
      removeIdFromList(namesTable, firstName, user.Id);
      usersToDelete.splice(usersToDelete.indexOf(index), 1);
      console.log(usersToDelete);
    }
  } else {
    console.log("No Users To Delete");
  }
}

setInterval(intervalFunc, 1500);

GetUsersDateFromCsv();
