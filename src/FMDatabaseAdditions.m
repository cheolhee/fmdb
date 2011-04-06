//
//  FMDatabaseAdditions.m
//  fmkit
//
//  Created by August Mueller on 10/30/05.
//  Copyright 2005 Flying Meat Inc.. All rights reserved.
//

#import "FMDatabase.h"
#import "FMDatabaseAdditions.h"

@implementation FMDatabase (FMDatabaseAdditions)

#define RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(type, sel)             \
va_list args;                                                        \
va_start(args, query);                                               \
FMResultSet *resultSet = [self executeQuery:query withArgumentsInArray:0x00 orVAList:args];   \
va_end(args);                                                        \
if (![resultSet next]) { return (type)0; }                           \
type ret = [resultSet sel:0];                                        \
[resultSet close];                                                   \
[resultSet setParentDB:nil];                                         \
return ret;


- (NSString*)stringForQuery:(NSString*)query, ... {
    RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(NSString *, stringForColumnIndex);
}

- (int)intForQuery:(NSString*)query, ... {
    RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(int, intForColumnIndex);
}

- (long)longForQuery:(NSString*)query, ... {
    RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(long, longForColumnIndex);
}

- (BOOL)boolForQuery:(NSString*)query, ... {
    RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(BOOL, boolForColumnIndex);
}

- (double)doubleForQuery:(NSString*)query, ... {
    RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(double, doubleForColumnIndex);
}

- (NSData*)dataForQuery:(NSString*)query, ... {
    RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(NSData *, dataForColumnIndex);
}

- (NSDate*)dateForQuery:(NSString*)query, ... {
    RETURN_RESULT_FOR_QUERY_WITH_SELECTOR(NSDate *, dateForColumnIndex);
}


//check if table exist in database (patch from OZLB)
- (BOOL)tableExists:(NSString*)tableName {
    
    BOOL returnBool;
    //lower case table name
    tableName = [tableName lowercaseString];
    //search in sqlite_master table if table exists
    FMResultSet *rs = [self executeQuery:@"select [sql] from sqlite_master where [type] = 'table' and lower(name) = ?", tableName];
    //if at least one next exists, table exists
    returnBool = [rs next];
    //close and free object
    [rs close];
    
    return returnBool;
}

//get table with list of tables: result colums: type[STRING], name[STRING],tbl_name[STRING],rootpage[INTEGER],sql[STRING]
//check if table exist in database  (patch from OZLB)
- (FMResultSet*)getSchema {
    
    //result colums: type[STRING], name[STRING],tbl_name[STRING],rootpage[INTEGER],sql[STRING]
    FMResultSet *rs = [self executeQuery:@"SELECT type, name, tbl_name, rootpage, sql FROM (SELECT * FROM sqlite_master UNION ALL SELECT * FROM sqlite_temp_master) WHERE type != 'meta' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, type DESC, name"];
    
    return rs;
}

//get table schema: result colums: cid[INTEGER], name,type [STRING], notnull[INTEGER], dflt_value[],pk[INTEGER]
- (FMResultSet*)getTableSchema:(NSString*)tableName {
    
    //result colums: cid[INTEGER], name,type [STRING], notnull[INTEGER], dflt_value[],pk[INTEGER]
    FMResultSet *rs = [self executeQuery:[NSString stringWithFormat: @"PRAGMA table_info(%@)", tableName]];
    
    return rs;
}


//check if column exist in table
- (BOOL)columnExists:(NSString*)tableName columnName:(NSString*)columnName {
    
    BOOL returnBool = NO;
    //lower case table name
    tableName = [tableName lowercaseString];
    //lower case column name
    columnName = [columnName lowercaseString];
    //get table schema
    FMResultSet *rs = [self getTableSchema: tableName];
    //check if column is present in table schema
    while ([rs next]) {
        if ([[[rs stringForColumn:@"name"] lowercaseString] isEqualToString: columnName]) {
            returnBool = YES;
            break;
        }
    }
    //close and free object
    [rs close];
    
    return returnBool;
}

+ (id)databaseWithPathInBundle:(NSString*)inPathInBundle {
	NSString *fullPath = [[[NSBundle mainBundle] bundlePath] 
						  stringByAppendingPathComponent:inPathInBundle];	
	return [[[self alloc] initWithPath:fullPath] autorelease];
}


+ (id)databaseWithPathInDocuments:(NSString*)path {
	NSString* documentsPath;
	NSArray* dirs = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
	documentsPath = [dirs objectAtIndex:0];
	NSString *fullPath = [documentsPath stringByAppendingPathComponent:path];	
	return [[[self alloc] initWithPath:fullPath] autorelease];
}

-(NSArray*)resultSetWithSQL:(NSString*)sql args:(NSArray*)args columns:(NSArray*)cols {
	NSMutableArray *ar = [[NSMutableArray alloc] init];
	NSAutoreleasePool *p = [[NSAutoreleasePool alloc] init];
	FMResultSet *rs = [self executeQuery:sql withArgumentsInArray:args];
	NSArray *cs;
	if (cols==nil) {
		cs = [rs columns];
	} else {
		cs = cols;
	}
	while ([rs next]) {
		NSDictionary *d = [rs dictionaryForColumns:cs];
		if (d!=nil) {
			[ar addObject:d];
		}
	}
	[rs close];
	[p drain];
	return [ar autorelease];
}


@end



@implementation FMResultSet(NECommons)
-(NSArray*)columns {
	return [columnNameToIndexMap allKeys];
}

-(NSDictionary*)dictionaryForColumns:(NSArray*)columns {
	[columns retain];
	NSMutableDictionary *d = [[NSMutableDictionary alloc] init];
	for (NSString *c in columns) {
		id v = [self stringForColumn:c];
		if (v==nil) {
			v = [NSNull null];
		}
		[d setObject:v forKey:c];
	}
	[columns release];
	return [d autorelease];
}

@end

