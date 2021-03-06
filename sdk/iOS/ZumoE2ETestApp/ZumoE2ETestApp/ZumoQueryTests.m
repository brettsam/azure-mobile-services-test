// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

#import "ZumoQueryTests.h"
#import <WindowsAzureMobileServices/WindowsAzureMobileServices.h>
#import "ZumoTestGlobals.h"
#import "ZumoTest.h"
#import "ZumoQueryTestData.h"

@interface OrderByClause : NSObject

@property (nonatomic, copy) NSString *fieldName;
@property (nonatomic) BOOL isAscending;

+(id)ascending:(NSString *)fieldName;
+(id)descending:(NSString *)fieldName;

@end

@implementation OrderByClause

@synthesize fieldName, isAscending;

+(id)ascending:(NSString *)field {
    OrderByClause *result = [[OrderByClause alloc] init];
    [result setFieldName:field];
    [result setIsAscending:YES];
    return result;
}

+(id)descending:(NSString *)field {
    OrderByClause *result = [[OrderByClause alloc] init];
    [result setFieldName:field];
    [result setIsAscending:NO];
    return result;
}

@end

@implementation ZumoQueryTests

static NSString *queryTestsTableName = @"intIdMovies";
static NSString *stringIdQueryTestsTableName = @"Movies";

+ (NSArray *)createTests {
    NSMutableArray *result = [[NSMutableArray alloc] init];
    
    [self addQueryTestToGroup:result name:@"GreaterThan and LessThan - Movies from the 90s" predicate:[NSPredicate predicateWithFormat:@"(year > 1989) and (year < 2000)"]];

    // Numeric fields
    [self addQueryTestToGroup:result name:@"GreaterThan and LessThan - Movies from the 90s" predicate:[NSPredicate predicateWithFormat:@"(year > 1989) and (year < 2000)"]];
    [self addQueryTestToGroup:result name:@"GreaterEqual and LessEqual - Movies from the 90s" predicate:[NSPredicate predicateWithFormat:@"(year >= 1990) and (year <= 1999)"]];
    [self addQueryTestToGroup:result name:@"Compound statement - OR of ANDs - Movies from the 30s and 50s" predicate:[NSPredicate predicateWithFormat:@"((year >= 1930) && (year < 1940)) || ((year >= 1950) && (year < 1960))"]];
    [self addQueryTestToGroup:result name:@"Division, equal and different - Movies from the year 2000 with rating other than R" predicate:[NSPredicate predicateWithFormat:@"((year / 1000.0) = 2) and (mpaaRating != 'R')"]];
    [self addQueryTestToGroup:result name:@"Addition, subtraction, relational, AND - Movies from the 1980s which last less than 2 hours" predicate:[NSPredicate predicateWithFormat:@"((year - 1900) >= 80) and (year + 10 < 2000) and (duration < 120)"]];
    
    // String functions
    [self addQueryTestToGroup:result name:@"StartsWith - Movies which starts with 'The'" predicate:[NSPredicate predicateWithFormat:@"title BEGINSWITH %@", @"The"] top:@100 skip:nil];
    [self addQueryTestToGroup:result name:@"StartsWith, case insensitive - Movies which start with 'the'" predicate:[NSPredicate predicateWithFormat:@"title BEGINSWITH[c] %@", @"the"] top:@100 skip:nil];
    [self addQueryTestToGroup:result name:@"EndsWith, case insensitive - Movies which end with 'r'" predicate:[NSPredicate predicateWithFormat:@"title ENDSWITH[c] 'r'"]];
    [self addQueryTestToGroup:result name:@"Contains - Movies which contain the word 'one', case insensitive" predicate:[NSPredicate predicateWithFormat:@"title CONTAINS[c] %@", @"one"]];
    [self addQueryTestToGroup:result name:@"Contains (non-ASCII) - Movies containing the 'é' character" predicate:[NSPredicate predicateWithFormat:@"title CONTAINS[c] 'é'"]];
    
    // String fields
    [self addQueryTestToGroup:result name:@"Equals - Movies since 1980 with rating PG-13" predicate:[NSPredicate predicateWithFormat:@"mpaaRating = 'PG-13' and year >= 1980"] top:@100 skip:nil];
    [self addQueryTestToGroup:result name:@"Comparison to nil - Movies since 1980 without a MPAA rating" predicate:[NSPredicate predicateWithFormat:@"mpaaRating = %@ and year >= 1980", nil]];
    [self addQueryTestToGroup:result name:@"Comparison to nil (not NULL) - Movies before 1970 with a MPAA rating" predicate:[NSPredicate predicateWithFormat:@"mpaaRating <> %@ and year < 1970", nil]];

    // Numeric functions
    [self addQueryTestToGroup:result name:@"Floor - Movies which last more than 3 hours" predicate:[NSPredicate predicateWithFormat:@"floor(duration / 60.0) >= 3"]];
    [self addQueryTestToGroup:result name:@"Ceiling - Best picture winners which last at most 2 hours" predicate:[NSPredicate predicateWithFormat:@"bestPictureWinner = TRUE and ceiling(duration / 60.0) = 2"]];
    
    // Constant predicates
    [self addQueryTestToGroup:result name:@"TRUEPREDICATE - First 10 movies" predicate:[NSPredicate predicateWithFormat:@"TRUEPREDICATE"] top:@10 skip:nil];
    [self addQueryTestToGroup:result name:@"FALSEPREDICATE - No movies" predicate:[NSPredicate predicateWithFormat:@"FALSEPREDICATE"]];

    // Date fields
    [self addQueryTestToGroup:result name:@"Date: Greater than, less than - Movies with release date in the 70s" predicate:[NSPredicate predicateWithFormat:@"releaseDate > %@ and releaseDate < %@", [ZumoTestGlobals createDateWithYear:1969 month:12 day:31], [ZumoTestGlobals createDateWithYear:1980 month:1 day:1]]];
    [self addQueryTestToGroup:result name:@"Date: Greater or equal, less or equal - Movies with release date in the 80s" predicate:[NSPredicate predicateWithFormat:@"releaseDate >= %@ and releaseDate <= %@", [ZumoTestGlobals createDateWithYear:1980 month:1 day:1], [ZumoTestGlobals createDateWithYear:1989 month:12 day:31]]];
    [self addQueryTestToGroup:result name:@"Date: Equal - Movies released on 1994-10-14 (Shawshank Redemption / Pulp Fiction)" predicate:[NSPredicate predicateWithFormat:@"releaseDate = %@", [ZumoTestGlobals createDateWithYear:1994 month:10 day:14]]];

    // Bool fields
    [self addQueryTestToGroup:result name:@"Bool: equal to TRUE - Best picture winners before 1950" predicate:[NSPredicate predicateWithFormat:@"bestPictureWinner = TRUE and year < 1950"]];
    [self addQueryTestToGroup:result name:@"Bool: equal to FALSE - Best picture winners after 2000" predicate:[NSPredicate predicateWithFormat:@"not(bestPictureWinner = FALSE) and year >= 2000"]];
    [self addQueryTestToGroup:result name:@"Bool: not equal to FALSE - Best picture winners after 2000" predicate:[NSPredicate predicateWithFormat:@"bestPictureWinner != FALSE and year >= 2000"]];
    
    // Predicate with substitution variables
    [self addQueryTestToGroup:result name:@"IN - Movies from the even years in the 2000s with rating PG, PG-13 or R" predicate:[NSPredicate predicateWithFormat:@"year IN %@ and mpaaRating IN %@", @[@2000, @2002, @2004, @2006, @2008], @[@"R", @"PG", @"PG-13"]] top:@100 skip:nil];
    [self addQueryTestToGroup:result name:@"BETWEEN - Movies from the 1960s" predicate:[NSPredicate predicateWithFormat:@"year BETWEEN %@", @[@1960, @1970]]];
    [self addQueryTestToGroup:result name:@"%K, %d substitution - Movies from 2000 rated PG-13" predicate:[NSPredicate predicateWithFormat:@"%K >= %d and %K = %@", @"year", @2000, @"mpaaRating", @"PG-13"]];

    // Top and skip
    [self addQueryTestToGroup:result name:@"Get all using large $top - fetchLimit = 500" predicate:nil top:@500 skip:nil];
    [self addQueryTestToGroup:result name:@"Skip all using large $skip - fetchOffset = 500" predicate:nil top:nil skip:@500];
    [self addQueryTestToGroup:result name:@"Skip, take and includeTotalCount - Movies 11-20, ordered by title" predicate:nil top:@10 skip:@10 orderBy:@[[OrderByClause ascending:@"title"]] includeTotalCount:YES selectFields:nil];
    [self addQueryTestToGroup:result name:@"Skip, take and includeTotalCount with predicate - Movies 11-20 which won the best picture award, ordered by release date" predicate:[NSPredicate predicateWithFormat:@"bestPictureWinner = TRUE"] top:@10 skip:@10 orderBy:[NSArray arrayWithObject:[OrderByClause descending:@"year"]] includeTotalCount:YES selectFields:nil];
    
    // Order by
    [self addQueryTestToGroup:result name:@"Order by date and string - 50 movies, ordered by release date, then title" predicate:nil top:@50 skip:nil orderBy:@[[OrderByClause descending:@"releaseDate"], [OrderByClause ascending:@"title"]] includeTotalCount:NO selectFields:nil];
    [self addQueryTestToGroup:result name:@"Order by number - 30 shorter movies since 1970" predicate:[NSPredicate predicateWithFormat:@"year >= 1970"] top:@30 skip:nil orderBy:[NSArray arrayWithObjects:[OrderByClause ascending:@"duration"], [OrderByClause ascending:@"title"], nil] includeTotalCount:YES selectFields:nil];
    
    // Select
    [self addQueryTestToGroup:result name:@"Select single field - Title of movies since 2000" predicate:[NSPredicate predicateWithFormat:@"year >= 2000"] top:@200 skip:nil orderBy:nil includeTotalCount:NO selectFields:@[@"title"]];
    [self addQueryTestToGroup:result name:@"Select multiple fields - Title, BestPictureWinner, Duration, ordered by release date, movies from the 1990" predicate:[NSPredicate predicateWithFormat:@"year >= 1990 and year < 2000"] top:@300 skip:nil orderBy:@[[OrderByClause ascending:@"title"]] includeTotalCount:NO selectFields:@[@"title", @"bestPictureWinner", @"duration"]];
    
    for (int i = -1; i <= 0; i++) {
        ZumoTest *negativeLookupTest = [ZumoTest createTestWithName:[NSString stringWithFormat:@"(Neg) MSTable readWithId:%d", i] andExecution:^(ZumoTest *test, UIViewController *viewController, ZumoTestCompletion completion) {
            MSClient *client = [[ZumoTestGlobals sharedInstance] client];
            MSTable *table = [client tableWithName:queryTestsTableName];
            [table readWithId:[NSNumber numberWithInt:i] completion:^(NSDictionary *item, NSError *err) {
                BOOL passed = NO;
                if (err) {
                    if (i == 0) {
                        if (err.code != MSInvalidItemIdWithRequest) {
                            [test addLog:[NSString stringWithFormat:@"Invalid error code: %ld", (long)err.code]];
                        } else {
                            [test addLog:@"Got expected error"];
                            NSHTTPURLResponse *response = [[err userInfo] objectForKey:MSErrorResponseKey];
                            if (response) {
                                [test addLog:[NSString stringWithFormat:@"Error, response should be nil (request not sent), but its status code is %ld", (long)[response statusCode]]];
                                passed = NO;
                            } else {
                                [test addLog:@"Success, request was not sent to the server"];
                                passed = YES;
                            }
                        }
                    } else {
                        if (err.code != MSErrorMessageErrorCode) {
                            [test addLog:[NSString stringWithFormat:@"Invalid error code: %ld", (long)err.code]];
                        } else {
                            NSHTTPURLResponse *httpResponse = [[err userInfo] objectForKey:MSErrorResponseKey];
                            NSInteger statusCode = [httpResponse statusCode];
                            if (statusCode == 404) {
                                [test addLog:@"Got expected error"];
                                passed = YES;
                            } else {
                                [test addLog:[NSString stringWithFormat:@"Invalid response status code: %ld", (long)statusCode]];
                                passed = NO;
                            }
                        }
                    }
                } else {
                    [test addLog:[NSString stringWithFormat:@"Expected error for lookup with id:%d, but got data back: %@", i, item]];
                }
                
                [test setTestStatus:(passed ? TSPassed : TSFailed)];
                completion(passed);
            }];
        }];

        [negativeLookupTest addRequiredFeature:@"intIdTables"];
        [result addObject:negativeLookupTest];
    }
    
    [result addObject:[self createNegativeTestWithName:@"(Neg) Very large fetchOffset" andQuerySettings:^(MSQuery *query) {
        query.fetchLimit = 1000000;
    } andQueryValidation:^(ZumoTest *test, NSError *err) {
        if (err.code != MSErrorMessageErrorCode) {
            [test addLog:[NSString stringWithFormat:@"Invalid error code: %ld", (long)err.code]];
            return NO;
        } else {
            NSHTTPURLResponse *resp = [[err userInfo] objectForKey:MSErrorResponseKey];
            if ([resp statusCode] == 400) {
                return YES;
            } else {
                [test addLog:[NSString stringWithFormat:@"Incorrect response status code: %ld", (long)[resp statusCode]]];
                return NO;
            }
        }
    }]];
    
    NSArray *unsupportedPredicates = [NSArray arrayWithObjects:
                                      @"average(duration) > 120",
                                      @"predicate from block",
                                      nil];
    for (NSString *unsupportedPredicate in unsupportedPredicates) {
        ZumoTest *negTest = [ZumoTest createTestWithName:[NSString stringWithFormat:@"(Neg) Unsupported predicate: %@", unsupportedPredicate] andExecution:^(ZumoTest *test, UIViewController *viewController, ZumoTestCompletion completion) {
            MSClient *client = [[ZumoTestGlobals sharedInstance] client];
            MSTable *table = [client tableWithName:queryTestsTableName];
            NSPredicate *predicate;
            if ([unsupportedPredicate isEqualToString:@"predicate from block"]) {
                predicate = [NSPredicate predicateWithBlock:^BOOL(id evaluatedObject, NSDictionary *bindings) {
                    return [[(NSDictionary *)evaluatedObject objectForKey:@"BestPictureWinner"] boolValue];
                }];
            } else {
                predicate = [NSPredicate predicateWithFormat:unsupportedPredicate];
            }
            
            [table readWithPredicate:predicate completion:^(MSQueryResult *result, NSError *error) {
                BOOL passed = NO;
                if (!error) {
                    [test addLog:[NSString stringWithFormat:@"Expected error, got result: %@", result.items]];
                } else {
                    if ([error code] == MSPredicateNotSupported) {
                        [test addLog:[NSString stringWithFormat:@"Got expected error: %@", error]];
                        passed = YES;
                    } else {
                        [test addLog:[NSString stringWithFormat:@"Wrong error received: %@", error]];
                    }
                }
                
                [test setTestStatus:(passed ? TSPassed : TSFailed)];
                completion(passed);
            }];
        }];
        [negTest addRequiredFeature:@"intIdTables"];
        [result addObject:negTest];
    }
    
    return result;
}

+ (void)isAllDataPopulated:(MSTable *)table test:(ZumoTest *)test expectCount:(NSInteger)expectCount retryTimes:(NSInteger)retryTimes completion:(ZumoTestCompletion)completion {
    MSQuery *query = [table query];
    query.includeTotalCount = YES;
    query.fetchLimit = 0;
    
    if(retryTimes-- == 0)
    {
        [test addLog:@"Error populate table: Time out. Not populate enough data."];
        [test setTestStatus:TSFailed];
        completion(NO);
        return;
    }
    
    [query readWithCompletion:^(MSQueryResult *result, NSError *error) {
        if (!error) {
            if (result.totalCount == expectCount){
                [test addLog:@"Table is populated and ready for query tests"];
                [test setTestStatus:TSPassed];
                completion(YES);
                return;
            } else {
                [test addLog:[NSString stringWithFormat:@"Already inserted %zd items, waiting for insertion to complete", (ssize_t)result.totalCount]];
                [NSThread sleepForTimeInterval: 5];
                [self isAllDataPopulated:table test:test expectCount:expectCount retryTimes:retryTimes completion:completion];
            }
        } else {
            [test addLog:[NSString stringWithFormat:@"Error querying table: %@", error]];
            [test setTestStatus:TSFailed];
            completion(NO);
			return;
        }
    }];
}

typedef void (^ActionQuery)(MSQuery *query);
typedef BOOL (^QueryValidation)(ZumoTest *test, NSError *error);

+ (ZumoTest *)createNegativeTestWithName:(NSString *)name andQuerySettings:(ActionQuery)settings andQueryValidation:(QueryValidation)queryValidation {
    ZumoTest *result = [ZumoTest createTestWithName:name andExecution:^(ZumoTest *test, UIViewController *viewController, ZumoTestCompletion completion) {
        MSClient *client = [[ZumoTestGlobals sharedInstance] client];
        MSTable *table = [client tableWithName:queryTestsTableName];
        MSQuery *query = [table query];
        settings(query);
        
        [query readWithCompletion:^(MSQueryResult *result, NSError *error) {
            if (error) {
                if (queryValidation(test, error)) {
                    [test addLog:[NSString stringWithFormat:@"Got expected error: %@", error]];
                    [test setTestStatus:TSPassed];
                    completion(YES);
                } else {
                    [test addLog:[NSString stringWithFormat:@"Error wasn't the expected one: %@", error]];
                    [test setTestStatus:TSFailed];
                    completion(NO);
                }
            } else {
                [test addLog:@"Query should fail, but succeeded"];
                [test setTestStatus:TSFailed];
                completion(NO);
            }
        }];
    }];
    
    [result addRequiredFeature:@"intIdTables"];
    return result;
}

+ (void)addQueryTestToGroup:(NSMutableArray *)testGroup name:(NSString *)name predicate:(NSPredicate *)predicate {
    [self addQueryTestToGroup:testGroup name:name predicate:predicate top:nil skip:nil orderBy:nil includeTotalCount:NO selectFields:nil];
}

+ (void)addQueryTestToGroup:(NSMutableArray *)testGroup name:(NSString *)name predicate:(NSPredicate *)predicate top:(NSNumber *)top skip:(NSNumber *)skip {
    [self addQueryTestToGroup:testGroup name:name predicate:predicate top:top skip:skip orderBy:nil includeTotalCount:NO selectFields:nil];
}

+ (void)addQueryTestToGroup:(NSMutableArray *)testGroup name:(NSString *)name predicate:(NSPredicate *)predicate top:(NSNumber *)top skip:(NSNumber *)skip orderBy:(NSArray *)orderByClauses includeTotalCount:(BOOL)includeTotalCount selectFields:(NSArray *)selectFields {
    [testGroup addObject:[self createQueryTestWithName:name andPredicate:predicate andTop:top andSkip:skip andOrderBy:orderByClauses andIncludeTotalCount:includeTotalCount andSelectFields:selectFields useStringIdTable:NO]];
    [testGroup addObject:[self createQueryTestWithName:name andPredicate:predicate andTop:top andSkip:skip andOrderBy:orderByClauses andIncludeTotalCount:includeTotalCount andSelectFields:selectFields useStringIdTable:YES]];
}

+ (ZumoTest *)createQueryTestWithName:(NSString *)name andPredicate:(NSPredicate *)predicate andTop:(NSNumber *)top andSkip:(NSNumber *)skip andOrderBy:(NSArray *)orderByClauses andIncludeTotalCount:(BOOL)includeTotalCount andSelectFields:(NSArray *)selectFields useStringIdTable:(BOOL)useStringIdTable {
    NSString *testName = [NSString stringWithFormat:@"[%@ id] %@", useStringIdTable ? @"string" : @"int", name];
    ZumoTest *result = [ZumoTest createTestWithName:testName andExecution:^(ZumoTest *test, UIViewController *viewController, ZumoTestCompletion completion) {

        MSClient *client = [[ZumoTestGlobals sharedInstance] client];
        MSTable *table = [client tableWithName:useStringIdTable ? stringIdQueryTestsTableName : queryTestsTableName];
        NSArray *allItems = [ZumoQueryTestData getMovies];
        if (!top && !skip && !orderByClauses && !includeTotalCount && !selectFields) {
            // use simple readWithPredicate
            [table readWithPredicate:predicate completion:^(MSQueryResult *result, NSError *readWhereError) {
                if (readWhereError) {
                    [test addLog:[NSString stringWithFormat:@"Error calling readWhere: %@", readWhereError]];
                    [test setTestStatus:TSFailed];
                    completion(NO);
                } else {
                    NSArray *queriedItems = result.items;
                    NSArray *filteredArray = [allItems filteredArrayUsingPredicate:predicate];
                    
                    BOOL passed = [self compareExpectedArray:filteredArray withActual:queriedItems forTest:test];
                    NSInteger queriedCount = [queriedItems count];
                    NSInteger maxTrace = queriedCount > 5 ? 5 : queriedCount;
                    NSArray *toTrace = [queriedItems subarrayWithRange:NSMakeRange(0, maxTrace)];
                    NSString *continuation = queriedCount > 5 ? @" (and more items)" : @"";
                    [test addLog:[NSString stringWithFormat:@"Queried items: %@%@", toTrace, continuation]];
                    
                    if (passed) {
                        [test addLog:@"Test passed"];
                        [test setTestStatus:TSPassed];
                        completion(YES);
                    } else {
                        [test setTestStatus:TSFailed];
                        completion(NO);
                    }
                }
            }];
        } else {
            MSQuery *query = predicate ? [table queryWithPredicate:predicate] : [table query];
            if (top) {
                [query setFetchLimit:[top integerValue]];
            }

            if (skip) {
                [query setFetchOffset:[skip integerValue]];
            }
            
            if (orderByClauses) {
                for (OrderByClause *clause in orderByClauses) {
                    if ([clause isAscending]) {
                        [query orderByAscending:[clause fieldName]];
                    } else {
                        [query orderByDescending:[clause fieldName]];
                    }
                }
            }
            
            if (includeTotalCount) {
                [query setIncludeTotalCount:YES];
            }
            
            if (selectFields) {
                [query setSelectFields:selectFields];
            }
            
            [query readWithCompletion:^(MSQueryResult *result, NSError *queryReadError) {
                if (queryReadError) {
                    [test addLog:[NSString stringWithFormat:@"Error calling MSQuery readWithCompletion: %@", queryReadError]];
                    [test setTestStatus:TSFailed];
                    completion(NO);
                } else {
                    NSArray *filteredArray;
                    NSArray *queriedItems = result.items;
                    
                    if (predicate) {
                        filteredArray = [NSMutableArray arrayWithArray:[allItems filteredArrayUsingPredicate:predicate]];
                    } else {
                        filteredArray = [NSMutableArray arrayWithArray:allItems];
                    }
                    
                    BOOL passed = NO;
                    NSInteger expectedTotalItems = [filteredArray count];
                    
                    if (includeTotalCount && result.totalCount != expectedTotalItems) {
                        [test addLog:[NSString stringWithFormat:@"Error in 'totalCount'. Expected: %ld, actual: %ld", (long)expectedTotalItems, (long)result.totalCount]];
                    } else {
                        if (orderByClauses) {
                            if ([orderByClauses count] == 1 && [[orderByClauses[0] fieldName] isEqualToString:@"title"] && [orderByClauses[0] isAscending]) {
                                // Special case, need to ignore accents
                                filteredArray = [filteredArray sortedArrayUsingComparator:^NSComparisonResult(id obj1, id obj2) {
                                    NSString *title1 = obj1[@"title"];
                                    NSString *title2 = obj2[@"title"];
                                    return [title1 compare:title2 options:NSDiacriticInsensitiveSearch];
                                }];
                            } else {
                                NSMutableArray *sortDescriptors = [[NSMutableArray alloc] init];
                                for (OrderByClause *clause in orderByClauses) {
                                    [sortDescriptors addObject:[[NSSortDescriptor alloc] initWithKey:[clause fieldName] ascending:[clause isAscending]]];
                                }
                            
                                filteredArray = [filteredArray sortedArrayUsingDescriptors:sortDescriptors];
                            }
                        }

                        if (top || skip) {
                            NSInteger rangeStart = skip ? [skip intValue] : 0;
                            if (rangeStart > expectedTotalItems) {
                                rangeStart = expectedTotalItems;
                            }
                        
                            NSInteger rangeLen = top ? [top intValue] : expectedTotalItems;
                            if ((rangeStart + rangeLen) > expectedTotalItems) {
                                rangeLen = expectedTotalItems - rangeStart;
                            }
                            
                            filteredArray = [filteredArray subarrayWithRange:NSMakeRange(rangeStart, rangeLen)];
                        }
                        
                        if (selectFields) {
                            NSMutableArray *projectedArray = [[NSMutableArray alloc] init];
                            for (int i = 0; i < [filteredArray count]; i++) {
                                NSDictionary *item = filteredArray[i];
                                item = [item dictionaryWithValuesForKeys:selectFields];
                                [projectedArray addObject:item];
                            }
                            
                            filteredArray = projectedArray;
                        }

                        passed = [self compareExpectedArray:filteredArray withActual:queriedItems forTest:test];
                    }
                    
                    if (passed) {
                        [test addLog:@"Received expected result"];
                        [test setTestStatus:TSPassed];
                    } else {
                        [test setTestStatus:TSFailed];
                    }
                    
                    completion(passed);
                }
            }];
        }
    }];
    
    [result addRequiredFeature:(useStringIdTable ? @"stringIdTables" : @"intIdTables")];
    return result;
}

+ (NSDictionary *)lowercaseKeysForDictionary:(NSDictionary *)item {
    NSMutableDictionary *newItem = [[NSMutableDictionary alloc] init];
    for (NSString *key in [newItem keyEnumerator]) {
        [newItem setValue:[item objectForKey:key] forKey:[key lowercaseString]];
    }
    return newItem;
}

+ (BOOL)shouldNormalizeDictionaryKeys {
    NSDictionary *runtimeFeatures = [[[ZumoTestGlobals sharedInstance] globalTestParameters] objectForKey:RUNTIME_FEATURES_KEY];
    NSNumber *runtimeCamelCasesPropertyNames = [runtimeFeatures objectForKey:@"alwaysCamelCaseProperties"];
    return (runtimeCamelCasesPropertyNames && [runtimeCamelCasesPropertyNames boolValue]);
}

+ (BOOL)compareExpectedArray:(NSArray *)expectedItems withActual:(NSArray *)actualItems forTest:(__weak ZumoTest *)test {
    BOOL result = NO;
    NSInteger actualCount = [actualItems count];
    NSInteger expectedCount = [expectedItems count];
    if (actualCount != expectedCount) {
        [test addLog:[NSString stringWithFormat:@"Expected %ld items, but got %ld", (long)expectedCount, (long)actualCount]];
        [test addLog:[NSString stringWithFormat:@"Expected items: %@", expectedItems]];
        [test addLog:[NSString stringWithFormat:@"Actual items: %@", actualItems]];
    } else {
        BOOL allItemsEqual = YES;
        for (int i = 0; i < actualCount; i++) {
            NSDictionary *expectedItem = [expectedItems objectAtIndex:i];
            NSDictionary *actualItem = [actualItems objectAtIndex:i];
            if ([self shouldNormalizeDictionaryKeys]) {
                expectedItem = [self lowercaseKeysForDictionary:expectedItem];
                actualItem = [self lowercaseKeysForDictionary:actualItem];
            }
            BOOL allValuesEqual = YES;
            for (NSString *key in [expectedItem keyEnumerator]) {
                if ([key isEqualToString:@"id"]) continue; // don't care about id
                id expectedValue = [expectedItem objectForKey:key];
                id actualValue = [actualItem objectForKey:key];
                if (![expectedValue isEqual:actualValue]) {
                    allValuesEqual = NO;
                    [test addLog:[NSString stringWithFormat:@"Error comparing field %@ of item %d: expected - %@, actual - %@", key, i, expectedValue, actualValue]];
                    break;
                }
            }
            
            if (!allValuesEqual) {
                allItemsEqual = NO;
            }
        }
        
        if (allItemsEqual) {
            result = YES;
        }
    }
    
    return result;
}

+ (NSString *)groupDescription {
    return @"Tests for validating different query capabilities of the client SDK.";
}

@end
