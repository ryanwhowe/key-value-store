# php-keyvaluestore [![Build Status](https://travis-ci.com/ryanwhowe/php-keyvaluestore.svg?branch=1.0)](https://travis-ci.com/ryanwhowe/php-keyvaluestore)
This is a database backed key/value store setter and getter library with additional functionality

This extends the key/value store in several ways

- The ability to separate out values by groups
  - This allows group1.key1 to exist and not collide with group2.key1
- The ability to have key/series values
  - This works like a logging method with the ability to retrieve the last value set or the entire series
- The ability to have key/series of distinct values
  - This allows for only recording when a value changes as a logging method with the ability to retrieve the last value 
  set or the series

### warning
Keys are forced to be case-insensitive

#### To Do (1.0)
- [ ] Remove the grouping column from the results, it is redundant
- [ ] Expand the tests to test multiple value key grouping sets
- [ ] Force the Keys to be case-insensitive
