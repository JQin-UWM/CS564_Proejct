#include "catalog.h"
#include "query.h"
#include "utility.h"
#include <string.h>
#include <stdlib.h>
#include <iostream>
using namespace std;

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

const Status QU_Insert(const string & relation,
                       const int attrCnt,
                       const attrInfo attrList[])
{
    cout << "Doing QU_Insert" << endl;
    Status status;
    if (relation.empty() || attrCnt <= 0) return BADCATPARM;

    // Get relation schema info
    AttrDesc *attrs;
    int relAttrCnt;
    status = attrCat->getRelInfo(relation, relAttrCnt, attrs);
    if (status != OK) return status;

    // Check if attrCnt matches relAttrCnt and no NULLs are allowed
    if (attrCnt != relAttrCnt) {
        free(attrs);
        return BADCATPARM;
    }

    // Calculate record length
    int reclen = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        reclen += attrs[i].attrLen;
    }

    char *recordData = new char[reclen];
    memset(recordData, 0, reclen);

    // Match attributes and copy values with proper conversion
    for (int i = 0; i < relAttrCnt; i++) {
        bool found = false;
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
                if (attrList[j].attrValue == NULL) {
                    // NULLs not allowed
                    delete[] recordData;
                    free(attrs);
                    return BADCATPARM;
                }

                switch (attrs[i].attrType) {
                    case INTEGER: {
                        int val = atoi((char*)attrList[j].attrValue);
                        memcpy(recordData + attrs[i].attrOffset, &val, sizeof(int));
                        break;
                    }
                    case FLOAT: {
                        float val = (float)atof((char*)attrList[j].attrValue);
                        memcpy(recordData + attrs[i].attrOffset, &val, sizeof(float));
                        break;
                    }
                    case STRING: {
                        memset(recordData + attrs[i].attrOffset, 0, attrs[i].attrLen);
                        strncpy(recordData + attrs[i].attrOffset, (char*)attrList[j].attrValue, attrs[i].attrLen - 1);
                        break;
                    }
                    default: {
                        delete[] recordData;
                        free(attrs);
                        return BADCATPARM;
                    }
                }
                found = true;
                break;
            }
        }
        if (!found) {
            // No value for this attribute
            delete[] recordData;
            free(attrs);
            return BADCATPARM;
        }
    }

    // Insert into the relation
    InsertFileScan ifs(relation, status);
    if (status != OK) {
        delete[] recordData;
        free(attrs);
        return status;
    }

    Record rec;
    rec.data = recordData;
    rec.length = reclen;

    RID rid;
    status = ifs.insertRecord(rec, rid);

    delete[] recordData;
    free(attrs);

    return status;
}