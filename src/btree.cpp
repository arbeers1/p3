/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
  //Construct index file name
  std::ostringstream idxStr;
  idxStr << relationName << '.' << attrByteOffset;
  std::string indexName = idxStr.str(); 
 
  //Opens the file if it exists, otherwise a new index file is created
  //scanned via FileScan, and record is inserted.
  if(badgerdb::File::exists(indexName)){
    file = new BlobFile(indexName, false);
  }else{
    file = new BlobFile(indexName, true);
    FileScan fileScan(relationName, bufMgrIn);
    
    //Scan
    while(true){
      RecordId rid;  //Get the rid if it exists
      try{
        fileScan.scanNext(rid);
	//read record for key and insert to tree
        std::string recordStr =  fileScan.getRecord();
        const char *record = recordStr.c_str();
        int key = *((int *)(record + attrByteOffset));
        void* keyPtr = &key;
        insertEntry(keyPtr, rid);
      }catch(EndOfFileException&){
	break;
      }
    }
  }
 
  //setup instance vars
  bufMgr = bufMgrIn;
  BTreeIndex::attrByteOffset = attrByteOffset;
  rootPageNum = 0;
  currentPageNum = 0;
  
  //Initialize indexMetaPage
  PageId pageNo; Page *page;
  try{
    bufMgr->allocPage(file, pageNo, page);
    headerPageNum = pageNo;
    std::ostringstream metaStr;
    metaStr << relationName << ',' << attrByteOffset << ',' << attrType << ',' << 0;
    std::string meta = metaStr.str();
    page->insertRecord(meta); 
    bufMgr->unPinPage(file, pageNo, true);
  }catch(PagePinnedException&){
  }catch(BadBufferException&){}
  outIndexName = indexName;
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
  //Stops any scan if one is occuring
  if(scanExecuting){
    endScan();
  }

  //May need to check and unpin pages here 
  
  //Flushes and deconstructs file
  bufMgr->flushFile(file);
  delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  //Case if root has not been initialized
  if(rootPageNum == 0){
    
    //Create leaf page
    PageId pageNum; Page *leafPage; 
    bufMgr->allocPage(file, pageNum, leafPage);
    LeafNodeInt *leaf = (struct LeafNodeInt*)leafPage;
    leaf->keyArray[0] = *((int*)key);
    leaf->ridArray[0] = rid;
    bufMgr->unPinPage(file, pageNum, true);
    //Create root
    PageId rootNum; Page *rootPage;
    bufMgr->allocPage(file, rootNum, rootPage);
    NonLeafNodeInt *root = (struct NonLeafNodeInt*)rootPage;
    root->level = 1;
    root->keyArray[0] = *((int*)key);
    root->pageNoArray[1] = pageNum; //insert leaf page to right of key
    rootPageNum = rootNum;
    bufMgr->unPinPage(file, rootNum, true);
  }else{ //Case where a root exists, root is checked
    if(currentPageNum == 0){
      currentPageNum = rootPageNum;
    }
    Page *page;
    bufMgr->readPage(file, currentPageNum, page);
    NonLeafNodeInt *node = (struct NonLeafNodeInt*)page;
    //Scan the node for the proper page to recurse on.
    for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
      //Return left page if key is less than current key
      if(*((int*)key) < node->keyArray[i]){
        currentPageNum = node->pageNoArray[i];
      }
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
