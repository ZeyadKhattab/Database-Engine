# Database-Engine
A Simulation of a Database Engine using bitmap indices implemented in Java.
All the entries of the database are serialzied and then saved on disk, also the indices are serialized and saved.
# Getting Started
* Clone the repo and open it in a Java IDE
* in the project directory, create the following empty folders:
    * data
    * docs/bitmaps
    * docs/pages
# Functions:

## DBApp.createTable()
#### parameters:
* tableName
* clusteringKey 
* map<colName,type>


You cannot create 2 tables with the same name, if you want to delete a table, you can delete its related info the *data/metadata.csv* and *docs/pages* files.

All the entries in the table will be sorted according to the clustering key.
## DBAPP.insertIntoTable()
#### parameters:
* tableName
* map<colName,value>

The *Clustering key* must be part of the inserted elements.

## DBApp.deleteFromTable()
#### parameters:
* tableName
* map<colName,value>

This function deletes all rows that match *colName=value*.
# DBApp.createBitmapIndex()
#### parameters:
* tableName
* colName

This function creates a bitmap index of the column you choose.


## Debugging Functions:

### DBApp.readTables()
#### parameters:
* tableName

this functions prints in the console all pages of a table.
### Bitmap.readBitmap()
#### parameters:
* tableName
* colName

this functions prints the bitmap indices created for a table using the column specified, you should have called *DBApp.createBitmapIndex(tableName,colName)* before using this function.

# Examples:
* Check the file Main.java and comment/uncomment the calls to insert, delete, create and adjust the parameters in these functions.
* Be careful not to create the same table/bitmap index twice

# Contribution:
This project was created in May 2019 as a part of an undergraduate Databases Course.

The authors of this project are present in this [commit](https://github.com/ZeyadKhattab/Database-Engine/commit/3f0841a7656df0a8bb75276120470e34dc3fc89f)





