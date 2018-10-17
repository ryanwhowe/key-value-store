# php-keyvaluestore [![Travis (.com) branch](https://img.shields.io/travis/com/ryanwhowe/php-keyvaluestore/1.0.svg)](https://github.com/ryanwhowe/php-keyvaluestore) [![GitHub (pre-)release](https://img.shields.io/github/release/ryanwhowe/php-keyvaluestore/all.svg)](https://github.com/ryanwhowe/php-keyvaluestore) [![GitHub issues](https://img.shields.io/github/issues-raw/ryanwhowe/php-keyvaluestore.svg)](https://github.com/ryanwhowe/php-keyvaluestore) [![Test Coverage](https://img.shields.io/badge/Test%20Coverage-100%25-brightgreen.svg)](https://github.com/ryanwhowe/php-keyvaluestore)
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
- [x] Remove the grouping column from the results, it is redundant
- [x] Expand the tests to test multiple value key grouping sets
- [x] Force the Keys to be case-insensitive
