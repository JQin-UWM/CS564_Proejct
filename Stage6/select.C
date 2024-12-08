#include "catalog.h"
#include "query.h"
#include "utility.h"
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <iostream>
using namespace std;

// forward declaration
const Status ScanSelect(const string & result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

const Status QU_Select(const string & result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
    cout << "Doing QU_Select " << endl;

    // Ensure projCnt > 0
    if (projCnt <= 0) return BADCATPARM;

    // Get attributes info of the input relation
    string inRelName = projNames[0].relName;
    int inAttrCnt = 0;
    AttrDesc *inAttrs = NULL;
    Status status = attrCat->getRelInfo(inRelName, inAttrCnt, inAttrs);
    if (status != OK) return status;

    // Convert projNames[] to AttrDesc[]
    AttrDesc *projDescs = new AttrDesc[projCnt];
    for (int i = 0; i < projCnt; i++) {
        AttrDesc temp;
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, temp);
        if (status != OK) {
            delete[] projDescs;
            free(inAttrs);
            return status;
        }
        projDescs[i] = temp;
    }

    // If there is a selection condition, get its AttrDesc and convert the value
    AttrDesc *selAttrDesc = NULL;
    char *filterVal = NULL;
    if (attr != NULL) {
        selAttrDesc = new AttrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, *selAttrDesc);
        if (status != OK) {
            delete[] projDescs;
            free(inAttrs);
            delete selAttrDesc;
            return status;
        }

        filterVal = new char[selAttrDesc->attrLen];
        memset(filterVal, 0, selAttrDesc->attrLen);

        // Convert attrValue to proper type
        switch ((Datatype)selAttrDesc->attrType) {
            case INTEGER: {
                int val = atoi(attrValue);
                memcpy(filterVal, &val, sizeof(int));
                break;
            }
            case FLOAT: {
                float fval = (float)atof(attrValue);
                memcpy(filterVal, &fval, sizeof(float));
                break;
            }
            case STRING: {
                strncpy(filterVal, attrValue, selAttrDesc->attrLen);
                break;
            }
        }
    }

    // Compute the length of the output tuple
    int reclen = 0;
    for (int i = 0; i < projCnt; i++) {
        reclen += projDescs[i].attrLen;
    }

    // Perform selection using ScanSelect
    status = ScanSelect(result, projCnt, projDescs, selAttrDesc, op, filterVal, reclen);

    delete[] projDescs;
    free(inAttrs);
    if (selAttrDesc) delete selAttrDesc;
    if (filterVal) delete[] filterVal;

    return status;
}


const Status ScanSelect(const string & result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;

    // Open InsertFileScan on result relation
    InsertFileScan *iScan = new InsertFileScan(result, status);
    if (status != OK) {
        delete iScan;
        return status;
    }

    // Use projNames[0] to find the relation to scan
    string inRelName = projNames[0].relName;

    // Open HeapFileScan on input relation
    HeapFileScan *hfs = new HeapFileScan(inRelName, status);
    if (status != OK) {
        delete hfs;
        delete iScan;
        return status;
    }

    // Set scan conditions
    if (attrDesc != NULL) {
        status = hfs->startScan(attrDesc->attrOffset,
                                attrDesc->attrLen,
                                (Datatype)attrDesc->attrType,
                                (char*)filter,
                                op);
    } else {
        status = hfs->startScan(0, 0, (Datatype)0, NULL, EQ);
    }

    if (status != OK) {
        delete hfs;
        delete iScan;
        return status;
    }

    RID rid;
    Record rec;
    char *outRec = new char[reclen];

    // Iterate over qualifying tuples
    while ((status = hfs->scanNext(rid)) == OK) {
        status = hfs->getRecord(rec);
        if (status != OK) break;

        int offset = 0;
        // Project attributes
        for (int i = 0; i < projCnt; i++) {
            memcpy(outRec + offset, (char*)rec.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        Record newRec;
        newRec.data = outRec;
        newRec.length = reclen;
        RID nrid;
        status = iScan->insertRecord(newRec, nrid);
        if (status != OK) break;
    }

    if (status == FILEEOF) status = OK;

    hfs->endScan();
    delete hfs;
    delete iScan;
    delete[] outRec;

    return status;
}