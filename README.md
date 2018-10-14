# php-keyvaluestore
This is a database backed key/value store setter and getter library with additional functionality

This extends the key/value store in several ways

- The ability to separate out values by groups
  - This allows group1.key1 to exist and not collide with group2.key1
- The ability to have key/series values
  - This works like a logging method with the ability to retrieve the last value set or the entire series
- The ability to have key/series of distinct values
  - This allows for only recording when a value changes as a logging method with the ability to retrieve the last value 
  set or the series

#### To Do
- [x] Split out the different types that can be stored
- [x] Complete the code coverage documentation on the tests
- [ ] Expand the tests to test multiple value key grouping sets
- [ ] Documentation!
- [ ] Test crossing types and see what happens (use one for setting and another for getting)
- [x] Settle on a response for the setters
- [x] Settle on a response for the getters
- [x] Split out the Series functionality to an abstract base for the Series and Distinct classes
