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
    *file = badgerdb::BlobFile::open(indexName);
  }else{
    *file = badgerdb::BlobFile::create(indexName);
    FileScan scanner = badgerdb::FileScan(indexName, bufMgrIn);
    while(true){
      RecordId rid;  //Get the rid if it exists
      try{
        scanner.scanNext(rid);
      }catch(EndOfFileException&){
	break;
      }

      //read record for key and insert to tree
      std::string recordStr =  scanner.getRecord();
      const char *record = recordStr.c_str();
      int key = *((int *)(record ));
      void* keyPtr = &key;
      insertEntry(keyPtr, rid);
    }
  }
  //setup instance vars
  bufMgr = bufMgrIn;
  BTreeIndex::attrByteOffset = attrByteOffset;
  
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
