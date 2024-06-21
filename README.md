L-Store Database Implementation
-
Course: ECS 165A; Database Systems

Description: Python implementation of L-Store, an database design that combines the features of both an **analytical** database and a **transactional** database. For analytical purposes, data are stored using the columnar storage so it is easy to analyze specific columns of the database. 
Instead of an in-place update design, it stores the initial records into base pages, while updates of the records are appended as tail pages. This allows for an
easy update process necessary for a transactional database, and it also allows for users to access multiple versions of the records.

Commands
-
