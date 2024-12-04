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
    status = newPage->setNextPage(-1); // No next page
    if (status != OK) {
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);
        db.closeFile(file);
        return status;
    }

    hdrPage->recCnt = 0;
    hdrPage->pageCnt = 1;
    hdrPage->firstPage = hdrPage->lastPage = newPageNo;

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
    status = filePtr->getFirstPage(headerPageNo);
    if (status != OK) {
        db.closeFile(filePtr);
        returnStatus = status;
        return;
    }

    // Read the header page into the buffer pool
    status = bufMgr->readPage(filePtr, headerPageNo, (Page *&) headerPage);
    if (status != OK) {
        db.closeFile(filePtr);
        returnStatus = status;
        return;
    }
    hdrDirtyFlag = false;

    // Read the first data page into the buffer pool
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
    // Unpin current data page if pinned
    if (curPage != NULL) {
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    }
    if (headerPage != NULL) {
        bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    }

    // Close the file
    db.closeFile(filePtr);
}

// Return number of records in heap file
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
                curPage = NULL;
                curPageNo = 0;
                curDirtyFlag = false;
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
HeapFileScan::HeapFileScan(const string &name, Status &status)
    : HeapFile(name, status) {
    filter = NULL;
}

// Start scanning the heap file with given filter criteria
const Status HeapFileScan::startScan(const int offset_, const int length_,
                                     const Datatype type_, const char *filter_,
                                     const Operator op_) {
    if (!filter_) { // No filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        ((type_ == INTEGER && length_ != sizeof(int)) ||
         (type_ == FLOAT && length_ != sizeof(float))) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT &&
         op_ != NE)) {
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
    // Unpin last page of the scan
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
HeapFileScan::~HeapFileScan() { endScan(); }

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
            if (status != OK)
                return status;
        }
        // Restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // Read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
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
    int nextPageNo;
    Record rec;

    if (curPageNo < 0)
        return FILEEOF; // Already at EOF!

    // Special case of the first record of the first page of the file
    if (curPage == NULL) {
        // Need to get the first page of the file
        curPageNo = headerPage->firstPage;
        if (curPageNo == -1)
            return FILEEOF; // File is empty

        // Read the first page of the file
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
        curRec = NULLRID;

        // Get the first record off the page
        status = curPage->firstRecord(curRec);
        if (status == NORECORDS) {
            // Unpin the current page
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPageNo = -1; // In case called again
            curPage = NULL; // For endScan()
            return FILEEOF; // First page had no records
        }

        // Get the record
        status = curPage->getRecord(curRec, rec);
        if (status != OK)
            return status;

        // See if record matches predicate
        if (matchRec(rec)) {
            outRid = curRec;
            return OK;
        }
    }

    // Default case: already have a page pinned
    while (true) {
        // Get the next record on the current page
        status = curPage->nextRecord(curRec, nextRid);
        if (status == OK) {
            curRec = nextRid;

            // Get the record
            status = curPage->getRecord(curRec, rec);
            if (status != OK)
                return status;

            // See if record matches predicate
            if (matchRec(rec)) {
                outRid = curRec;
                return OK;
            }
        } else if (status == ENDOFPAGE || status == NORECORDS) {
            // Get the next page in the file
            status = curPage->getNextPage(nextPageNo);
            if (status != OK)
                return status;
            if (nextPageNo == -1)
                return FILEEOF; // End of file

            // Unpin the current page
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPage = NULL;

            // Read the next page
            curPageNo = nextPageNo;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
                return status;
            curDirtyFlag = false;
            curRec = NULLRID;

            // Get the first record off the page
            status = curPage->firstRecord(curRec);
            if (status == NORECORDS)
                continue; // Page has no records, continue to next page

            // Get the record
            status = curPage->getRecord(curRec, rec);
            if (status != OK)
                return status;

            // See if record matches predicate
            if (matchRec(rec)) {
                outRid = curRec;
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

    // Delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    if (status != OK)
        return status;
    curDirtyFlag = true;

    // Reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return OK;
}

// Mark the current page as dirty
const Status HeapFileScan::markDirty() {
    curDirtyFlag = true;
    return OK;
}

// Check if a record matches the filter criteria
const bool HeapFileScan::matchRec(const Record &rec) const {
    // No filtering requested
    if (!filter)
        return true;

    // See if offset + length is beyond end of record
    if ((offset + length) > rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
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
    case STRING:
        diff = strncmp((char *) rec.data + offset, filter, length);
        break;
    }

    switch (op) {
    case LT:
        return diff < 0;
    case LTE:
        return diff <= 0;
    case EQ:
        return diff == 0;
    case GTE:
        return diff >= 0;
    case GT:
        return diff > 0;
    case NE:
        return diff != 0;
    default:
        return false;
    }
}

// Constructor for InsertFileScan
InsertFileScan::InsertFileScan(const string &name, Status &status)
    : HeapFile(name, status) {
    // Ensure current page is the last page
    if ((curPage != NULL) && (curPageNo != headerPage->lastPage)) {
        Status unpinStatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (unpinStatus != OK) {
            cerr << "Error in unpin of data page during InsertFileScan constructor" << endl;
        }
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            cerr << "Error in readPage during InsertFileScan constructor" << endl;
        }
        curDirtyFlag = false;
    }
}

// Destructor for InsertFileScan
InsertFileScan::~InsertFileScan() {
    // Unpin last page of the scan
    if (curPage != NULL) {
        bufMgr->unPinPage(filePtr, curPageNo, true);
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid) {
    Page *newPage;
    int newPageNo;
    Status status;

    // Check for very large records
    if ((unsigned int) rec.length > PAGESIZE - DPFIXED) {
        return INVALIDRECLEN;
    }

    if (curPage == NULL) {
        // Make the last page the current page and read it from disk
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
    }

    // Try to add the record onto the current page
    status = curPage->insertRecord(rec, outRid);
    if (status == OK) {
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true; // Page is dirty
        return OK;
    } else if (status == NOSPACE) {
        // Current page is full; allocate a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK)
            return status;

        // Initialize the new page
        newPage->init(newPageNo);
        status = newPage->setNextPage(-1); // No next page
        if (status != OK) {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }

        // Link up new page appropriately
        status = curPage->setNextPage(newPageNo); // Set forward pointer
        if (status != OK) {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }

        // Update header page
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        // Unpin the old current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }

        // Make the new page the current page
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;

        // Insert the record into the new page
        status = curPage->insertRecord(rec, outRid);
        if (status == OK) {
            headerPage->recCnt++;
            return OK;
        } else {
            return status;
        }
    } else {
        return status;
    }
}
