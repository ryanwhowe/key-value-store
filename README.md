# php-keyvaluestore [![Travis (.com)](https://img.shields.io/travis/com/ryanwhowe/key-value-store.svg)](https://github.com/ryanwhowe/key-value-store) [![GitHub (pre-)release](https://img.shields.io/github/release/ryanwhowe/key-value-store/all.svg)](https://github.com/ryanwhowe/key-value-store) [![GitHub issues](https://img.shields.io/github/issues-raw/ryanwhowe/key-value-store.svg)](https://github.com/ryanwhowe/key-value-store) [![Codecov](https://img.shields.io/codecov/c/github/ryanwhowe/key-value-store.svg)](https://github.com/ryanwhowe/key-value-store)

This is a database backed key/value store setter and getter library with additional functionality

This extends the key/value store in several ways

- The ability to separate out values by groups
  - This allows group1.key1 to exist and not collide with group2.key1
- The ability to have key/series values
  - This works like a logging method with the ability to retrieve the last value set or the entire series
- The ability to have key/series of distinct values
  - This allows for only recording when a value changes as a logging method with the ability to retrieve the last value 
  set or the series

Please read the [wiki](https://github.com/ryanwhowe/php-keyvaluestore/wiki) for additional documentation on the classes and methods.

Please add any [issues](https://github.com/ryanwhowe/php-keyvaluestore/issues) for anything that does not appear to work as expected.

Please feel free to ask any questions if you think that this could be something useful to you.
