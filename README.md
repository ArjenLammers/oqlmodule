# Description
This module allows you to execute OQL queries from a microflow.It allows setting named parameters which will be properly escaped.

# Typical usage scenario
Query data from the Mendix database and the result will be mapped to a Mendix entity.
The example below is part of the Example module within the project, located on the GitHub repository.

Considering the following domain model:

One can execute a query obtaining all objects of ExamplePerson as a table. The rows within this table can be converted to non persistent entities for further usage. Therefore a ExamplePersonResult non persistent entity is modeled.

Parameters can be used using their corresponding actions (for each data type there's one activity defined:

 

All parameters should be added before executing an OQL query.

 

Adding an parameter requires:

The name of the parameter, without a $ (e.g. Gender).
The value.
Executing an OQL query requires:

The OQL statement.
A resulting entity (e.g. OQLExample.ExamplePersonResult).
Amount.
Offset.
 

After executing an OQL query, all previously set parameters are cleared.

The example query used to obtain the ExamplePersonResults are is:

``SELECT P.id ExamplePersonResult_ExamplePerson, P.Name Name, P.Number Number, P.DateOfBirth DateOfBirth, P.Age Age, P.LongAge LongAge, P.HeightInFloat HeightInFloat, P.HeightInDecimal HeightInDecimal, P.Active Active, P.Gender Gender FROM OQLExample.ExamplePerson P WHERE P.Active = $Active AND P.Age = $Age AND P.DateOfBirth = $DateOfBirth AND P.Gender = $Gender AND P.HeightInDecimal = $HeightInDecimal AND P.HeightInFloat = $HeightInFloat AND P.LongAge = $LongAge AND P.Name = $Name AND P.Number = $Number AND P/OQL.MarriedTo/OQL.ExamplePerson/ID = $MarriedTo``

In the example above, the resulting columns Name, Number, DateOfBirth, Age, etc. are mapped to their corresponding attributes in ExamplePersonResult. The column ExamplePersonResult_ExamplePerson is mapped to the association (so one can retrieve the original persistent entity if needed).

# Features and limitations

Named parameters (like Data Set functionality) to avoid injection.
Automatic mapping of result table to a list of objects (of a supplied entity).
Mapping of an ID column to an assocation.

# Dependencies
Mendix 6.9 or newer

# Installation
Download and import into your project.

# Known bugs
A mapped association should be mentioned without its module prefix in an OQL query (e.g. ExamplePersonResult_ExamplePerson instead of OQLExample.ExamplePersonResult_ExamplePerson).