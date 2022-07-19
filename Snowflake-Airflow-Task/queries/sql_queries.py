CREATE_TABLES_AND_STREAMS = """create or replace TABLE raw_table (
            "_ID" VARCHAR(16777216),
            "IOS_App_Id" NUMBER(38,0),
            "Title" VARCHAR(16777216),
            "Developer_Name" VARCHAR(16777216),
            "Developer_IOS_Id" FLOAT,
            "IOS_Store_Url" VARCHAR(16777216),
            "Seller_Official_Website" VARCHAR(16777216),
            "Age_Rating" VARCHAR(16777216),
            "Total_Average_Rating" FLOAT,
            "Total_Number_of_Ratings" FLOAT,
            "Average_Rating_For_Version" FLOAT,
            "Number_of_Ratings_For_Version" NUMBER(38,0),
            "Original_Release_Date" VARCHAR(16777216),
            "Current_Version_Release_Date" VARCHAR(16777216),
            "Price_USD" FLOAT,
            "Primary_Genre" VARCHAR(16777216),
            "All_Genres" VARCHAR(16777216),
            "Languages" VARCHAR(16777216),
            "Description" VARCHAR(16777216)
        );
        CREATE OR REPLACE TABLE stage_table LIKE raw_table;
        CREATE OR REPLACE TABLE master_table LIKE raw_table;
        CREATE OR REPLACE STREAM raw_stream ON TABLE raw_table;
        CREATE OR REPLACE STREAM stage_stream ON TABLE stage_table;"""

INSERT_STAGE_TABLE = """ insert into stage_table 
            select
            "_ID",
            "IOS_App_Id",
            "Title",
            "Developer_Name",
            "Developer_IOS_Id",
            "IOS_Store_Url",
            "Seller_Official_Website",
            "Age_Rating",
            "Total_Average_Rating",
            "Total_Number_of_Ratings",
            "Average_Rating_For_Version",
            "Number_of_Ratings_For_Version",
            "Original_Release_Date",
            "Current_Version_Release_Date",
            "Price_USD",
            "Primary_Genre",
            "All_Genres",
            "Languages",
            "Description"
             from raw_stream """

INSERT_MASTER_TABLE = """insert into master_table
            select 
            "_ID",
            "IOS_App_Id",
            "Title",
            "Developer_Name",
            "Developer_IOS_Id",
            "IOS_Store_Url",
            "Seller_Official_Website",
            "Age_Rating",
            "Total_Average_Rating",
            "Total_Number_of_Ratings",
            "Average_Rating_For_Version",
            "Number_of_Ratings_For_Version",
            "Original_Release_Date",
            "Current_Version_Release_Date",
            "Price_USD",
            "Primary_Genre",
            "All_Genres",
            "Languages",
            "Description"
             from stage_stream"""