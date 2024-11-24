#include "heapfile.h"
#include "error.h"

// Routine to create a heap file
const Status createHeapFile(const string fileName) {
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage = NULL;

    // Try opening the file; if it exists, return FILEEXISTS
    status = db.openFile(fileName, file);
    if (status == OK) {
        return FILEEXISTS;
    }

    // File does not exist; create a new file
    status = db.createFile(fileName);
    if (status != OK) {
        return status;
    }

    // Open the newly created file
    status = db.openFile(fileName, file);
    if (status != OK) {
        return status;
    }

    // Allocate the header page
    status = bufMgr->allocPage(file, hdrPageNo, newPage);
    if (status != OK) {
        db.closeFile(file);
        return status;
    }

    // Initialize the header page
    hdrPage = (FileHdrPage *) newPage;
    memset(hdrPage, 0, sizeof(FileHdrPage));
    strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);

    // Allocate the first data page
    status = bufMgr->allocPage(file, newPageNo, newPage);
    if (status != OK) {
        bufMgr->unPinPage(file, hdrPageNo, true);
        db.closeFile(file);
        return status;
    }

    // Initialize the data page and link it to the header page
    newPage->init(newPageNo);
    hdrPage->firstPage = newPageNo;
    hdrPage->lastPage = newPageNo;
    hdrPage->pageCnt = 2; // Includes header page and data page
    hdrPage->recCnt = 0;

    // Unpin the pages and mark them as dirty
    bufMgr->unPinPage(file, hdrPageNo, true);
    bufMgr->unPinPage(file, newPageNo, true);

    // Flush buffer pool before closing the file
    status = bufMgr->flushFile(file);
    if (status != OK) {
        cerr << "Error flushing buffer pool during createHeapFile" << endl;
        db.closeFile(file);
        return status;
    }

    // Close the file
    db.closeFile(file);
    return OK;
}

// Routine to destroy a heap file
const Status destroyHeapFile(const string fileName) {
    return (db.destroyFile(fileName));
}

// Constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus) {
    Status status;
    returnStatus = OK;

    // Open the file
    status = db.openFile(fileName, filePtr);
    if (status != OK) {
        returnStatus = status;
        return;
    }

    // Get the header page
    int firstPageNo;
    status = filePtr->getFirstPage(firstPageNo);
    if (status != OK) {
        db.closeFile(filePtr);
        returnStatus = status;
        return;
    }

    headerPageNo = firstPageNo;
    status = bufMgr->readPage(filePtr, headerPageNo, (Page *&) headerPage);
    if (status != OK) {
        db.closeFile(filePtr);
        returnStatus = status;
        return;
    }
    hdrDirtyFlag = false;

    // Pin the first data page
    curPageNo = headerPage->firstPage;
    status = bufMgr->readPage(filePtr, curPageNo, curPage);
    if (status != OK) {
        bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
        db.closeFile(filePtr);
        returnStatus = status;
        return;
    }
    curDirtyFlag = false;
    curRec = NULLRID;
}

// Destructor closes the file
HeapFile::~HeapFile() {
    if (curPage != NULL) {
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    }
    if (headerPage != NULL) {
        bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    }

    // Flush buffer pool
    Status status = bufMgr->flushFile(filePtr);
    if (status != OK) {
        cerr << "Error flushing buffer pool during HeapFile destructor" << endl;
    }

    // Close the file
    db.closeFile(filePtr);
}

// Return the number of records in the heap file
const int HeapFile::getRecCnt() const {
    return headerPage->recCnt;
}

// Retrieve an arbitrary record from a file
// If the record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned. Returns a pointer to the record via the rec parameter
const Status HeapFile::getRecord(const RID &rid, Record &rec) {
    Status status;

    // Check if the record is on the current page
    if (curPageNo != rid.pageNo) {
        // If there is a pinned page, unpin it
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) {
                return status;
            }
        }

        // Read the target page into the buffer pool
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) {
            return status;
        }
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }

    // Retrieve the record from the current page
    status = curPage->getRecord(rid, rec);
    if (status == OK) {
        curRec = rid;
    }
    return status;
}

// HeapFileScan constructor
HeapFileScan::HeapFileScan(const string &name,
                           Status &status) : HeapFile(name, status) {
    filter = NULL;
}

// Start scanning the heap file with given filter criteria
const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_) {
    if (!filter_) {
        // No filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE)) {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

// End scanning the heap file
const Status HeapFileScan::endScan() {
    Status status;
    // Unpin the last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

// Destructor for HeapFileScan
HeapFileScan::~HeapFileScan() {
    endScan();
}

// Mark the current scan state
const Status HeapFileScan::markScan() {
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

// Reset the scan to the marked state
const Status HeapFileScan::resetScan() {
    Status status;
    if (markedPageNo != curPageNo) {
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // Restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // Read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
    } else {
        curRec = markedRec;
    }
    return OK;
}

// Get the next record that matches the filter criteria
const Status HeapFileScan::scanNext(RID &outRid) {
    Status status;
    RID nextRid;
    Record rec;

    while (true) {
        // Retrieve the next record on the current page
        status = curPage->nextRecord(curRec, nextRid);

        if (status == ENDOFPAGE) {
            // If the end of the current page is reached, load the next page
            int nextPageNo;
            status = curPage->getNextPage(nextPageNo);

            if (status != OK || nextPageNo == -1) {
                return FILEEOF; // No more pages
            }

            // Unpin the current page
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);

            // Load the next page
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if (status != OK) {
                return status;
            }

            curPageNo = nextPageNo;
            curDirtyFlag = false;
            curRec = NULLRID;
        } else if (status == OK) {
            // Retrieve the record and check against the filter
            status = curPage->getRecord(nextRid, rec);
            if (status == OK && matchRec(rec)) {
                outRid = nextRid;
                curRec = nextRid;
                return OK;
            }
        } else {
            return status;
        }
    }
}

// Retrieve the current record in the scan
const Status HeapFileScan::getRecord(Record &rec) {
    return curPage->getRecord(curRec, rec);
}

// Delete the current record in the scan
const Status HeapFileScan::deleteRecord() {
    Status status;
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // Update the record count in the header page
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// Mark the current page as dirty
const Status HeapFileScan::markDirty() {
    curDirtyFlag = true;
    return OK;
}

// Check if a record matches the filter criteria
const bool HeapFileScan::matchRec(const Record &rec) const {
    if (!filter) return true;

    if ((offset + length) > rec.length) {
        return false;
    }

    float diff = 0;
    switch (type) {
        case INTEGER: {
            int iattr, ifltr;
            memcpy(&iattr, (char *) rec.data + offset, sizeof(int));
            memcpy(&ifltr, filter, sizeof(int));
            diff = iattr - ifltr;
            break;
        }
        case FLOAT: {
            float fattr, ffltr;
            memcpy(&fattr, (char *) rec.data + offset, sizeof(float));
            memcpy(&ffltr, filter, sizeof(float));
            diff = fattr - ffltr;
            break;
        }
        case STRING: {
            diff = strncmp((char *) rec.data + offset, filter, length);
            break;
        }
    }

    switch (op) {
        case LT: return diff < 0;
        case LTE: return diff <= 0;
        case EQ: return diff == 0;
        case GTE: return diff >= 0;
        case GT: return diff > 0;
        case NE: return diff != 0;
        default: return false;
    }
}

// Constructor for InsertFileScan
InsertFileScan::InsertFileScan(const string &name, Status &status) : HeapFile(name, status) {
}

// Destructor for InsertFileScan
InsertFileScan::~InsertFileScan() {
    if (curPage != NULL) {
        bufMgr->unPinPage(filePtr, curPageNo, true);
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid) {
    Page *newPage;
    int newPageNo;
    Status status;

    if ((unsigned int) rec.length > PAGESIZE - DPFIXED) {
        return INVALIDRECLEN;
    }

    if (curPage == NULL) {
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK) {
            return status;
        }
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;
    }

    status = curPage->insertRecord(rec, outRid);
    if (status == NOSPACE) {
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) {
            return status;
        }

        newPage->init(newPageNo);
        newPage->setNextPage(-1);
        curPage->setNextPage(newPageNo);

        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;

        status = curPage->insertRecord(rec, outRid);
    }

    if (status == OK) {
        headerPage->recCnt++;
        hdrDirtyFlag = true;
    }

    return status;
}
