#include "catalog.h"
#include "query.h"
#include "utility.h"
#include <string.h>
#include <stdlib.h>
#include <iostream>
using namespace std;

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

const Status QU_Delete(const string & relation,
                       const string & attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)
{
    cout << "Doing QU_Delete" << endl;
    Status status;

    // Open relation for scanning
    HeapFileScan *hfs = new HeapFileScan(relation, status);
    if (status != OK) {
        delete hfs;
        return status;
    }

    int resultTupCnt = 0;

    if (!attrName.empty()) {
        AttrDesc ad;
        status = attrCat->getInfo(relation, attrName, ad);
        if (status != OK) {
            delete hfs;
            return status;
        }

        // Convert attrValue to appropriate type
        const char *filter;
        int tmpInt;
        float tmpFloat;

        switch (type) {
            case INTEGER: {
                tmpInt = atoi(attrValue);
                filter = (char*)&tmpInt;
                break;
            }
            case FLOAT: {
                tmpFloat = (float)atof(attrValue);
                filter = (char*)&tmpFloat;
                break;
            }
            case STRING: {
                filter = attrValue;
                break;
            }
        }

        status = hfs->startScan(ad.attrOffset, ad.attrLen, type, filter, op);

    } else {
        // Unconditional delete
        status = hfs->startScan(0, 0, (Datatype)0, NULL, EQ);
    }

    if (status != OK) {
        delete hfs;
        return status;
    }

    RID rid;
    while ((status = hfs->scanNext(rid)) == OK) {
        // Delete current record
        status = hfs->deleteRecord();
        if (status != OK) break;
        resultTupCnt++;
    }

    if (status == FILEEOF) status = OK;
    hfs->endScan();
    delete hfs;
    return status;
}