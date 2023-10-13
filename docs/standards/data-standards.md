## Data Standards ##

### IngestionDate ###
It indicates the moment from when the record is active, and it exists (together with ValidTo) to be able to calculate what record was active at a certain moment in time.

### ValidTo ###
It indicates the moment the record is inactive, and it exists (together with ingestionDate) to be able to calculate what record was active at a certain moment in time.

### IsActive ###

The use of IsActive flags should only be used to indicate the status of a row of data and never to indicate the status of an entity being described by a row of data.

For example, 
If I have a row of data that identifies an Employee using EmployeeID = 1 
and I recieve a new row of data about EmployeeID = 1, such as their role has changed from Role A to 
Role B then I will set IsActive = N on the original row and IsActive = Y on the new row.

This is used to tell the user which row is the latest row for a particular entity such as an employee.
