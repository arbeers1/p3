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
#include <climits>


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
 
  //setup instance vars
  bufMgr = bufMgrIn;
  BTreeIndex::attrByteOffset = attrByteOffset;
  rootPageNum = 0;
  currentPageNum = 0;
  nextEntry = -1;
  scanExecuting = false;
  
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
    //Create leaf page which will host the key,rid
    PageId pageNum; Page *leafPage; 
    bufMgr->allocPage(file, pageNum, leafPage);
    LeafNodeInt *leafRight = (struct LeafNodeInt*)leafPage;

    //Create the hosts left sibling
    PageId leftPageNum; Page *leftLeafPage;
    bufMgr->allocPage(file, leftPageNum, leftLeafPage);
    LeafNodeInt *leafLeft = (struct LeafNodeInt*)leftLeafPage;

    //Initialize leaf keys to max int for scanning purposes
    for(int i = 0; i < INTARRAYLEAFSIZE; i++){
      leafRight->keyArray[i] =  INT_MAX;
      leafLeft->keyArray[i] = INT_MAX;
    }
    leafRight->keyArray[0] = *((int*)key);
    leafRight->ridArray[0] = rid;
    leafRight->rightSibPageNo = 0;
    leafLeft->rightSibPageNo = pageNum;
    bufMgr->unPinPage(file, pageNum, true);
    bufMgr->unPinPage(file, leftPageNum, true);
    
    //Create root
    PageId rootNum; Page *rootPage;
    bufMgr->allocPage(file, rootNum, rootPage);
    NonLeafNodeInt *root = (struct NonLeafNodeInt*)rootPage;
    
    //Initialize non leaf keys to max int for scanning purposes
    for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
      root->keyArray[i] = INT_MAX;
    }
    root->pageNoArray[INTARRAYNONLEAFSIZE] = INT_MAX; //Fill in remaining slot
    root->level = 1;
    root->keyArray[0] = *((int*)key);
    root->pageNoArray[0] = leftPageNum;
    root->pageNoArray[1] = pageNum; //insert leaf page to right of key
    rootPageNum = rootNum;
    bufMgr->unPinPage(file, rootNum, true);

  }else{ //Case where a root exists, nodes are recursively checked
    int propKey; PageId propPageNo;
    insertHelper(rootPageNum, key, rid, propKey, propPageNo);
  }
}

bool BTreeIndex::insertHelper(PageId currentNum, const void *key, const RecordId rid, int &propKey, PageId &propPageNo){
  Page *page;
  bufMgr->readPage(file, currentNum, page);
  bufMgr->unPinPage(file, currentNum, false);
  NonLeafNodeInt *node = (struct NonLeafNodeInt*)page;

  //Scan the node for the proper page to recurse on.
  int childNum = 0;
  for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
    //Return left page if key is less than current key
    if(*((int*)key) < node->keyArray[i]){
      childNum = node->pageNoArray[i];
      break;
    }
  }

  //Check if a (left)page to recurse on was found, if not the last page is recursed on
  if(childNum == 0) childNum = node->pageNoArray[INTARRAYNONLEAFSIZE];

  //If next level is a leaf then check it, otherwise recurse
  if(node->level == 1){
    if(insertToLeaf(childNum, key, rid)){
      return true; //Case if leaf has space, insertion occurs, all is done
    }
    //Case if leaf needs to split
    Page *child; 
    bufMgr->readPage(file, childNum, child);
    LeafNodeInt *childNode = (struct LeafNodeInt*)child;
    splitLeaf(childNode, childNum, key, rid, propKey, propPageNo);
    bufMgr->unPinPage(file, childNum, true);
    //propogate key upwards
    bufMgr->readPage(file, currentNum, page);
    if(node->keyArray[INTARRAYNONLEAFSIZE - 1] == INT_MAX){ //parent node has space add propogated key/page
      insertNonLeaf(node, &propKey, propPageNo);
      bufMgr->unPinPage(file, currentNum, true);
      return true;
    }else{//parent node is full, split node, return false so that previous call can split it
          //if root though split here
      if(currentNum == rootPageNum){
        splitNonLeaf(node, &propKey, propPageNo, propKey, propPageNo, true);
	bufMgr->unPinPage(file, currentNum, true);
	return true;
      }
      return false;
    }
  }else{//recurse further down on first pass, split if needed on seconds pass upwards
    if(insertHelper(childNum, key, rid, propKey, propPageNo)){
      return true;
    }
   
    //case where split is needed
    bufMgr->readPage(file, childNum, page);
    NonLeafNodeInt *child = (struct NonLeafNodeInt*)page;
    splitNonLeaf(child, &propKey, propPageNo,propKey, propPageNo, false);
    bufMgr->unPinPage(file, childNum, true);
    
    //insert in parent if available 
    //if root is parent, insert if empty otherwise split
    bufMgr->readPage(file, currentNum, page);
    NonLeafNodeInt *node = (struct NonLeafNodeInt*)page;
    if(node->keyArray[INTARRAYNONLEAFSIZE-1] == INT_MAX){
      insertNonLeaf(node, &propKey, propPageNo);
      bufMgr->unPinPage(file, currentNum, true);
    }else{
      if(currentNum == rootPageNum){
        splitNonLeaf(node, &propKey, propPageNo, propKey, propPageNo, true);
        bufMgr->unPinPage(file, currentNum, true);
        return true;
      }
      return false;
    }
    
  }
  return false;
}

bool BTreeIndex::insertToLeaf(const PageId pageNum, const void *key, const RecordId rid){
      Page *leafPage;
      bufMgr->readPage(file, pageNum, leafPage);
      LeafNodeInt *leaf = (struct LeafNodeInt*)leafPage;

      //Check if space is available to insert into leaf
      //Insert if so, otherwise split
      if(leaf->keyArray[INTARRAYLEAFSIZE - 1] == INT_MAX){ 
        
	//Find the correct index to insert to
	int insertIndex;
        for(int i = 0; i < INTARRAYLEAFSIZE; i++){
          if(*((int*)key) < leaf->keyArray[i]){
            insertIndex = i;
	    break;
          }
        }

	//Shift any elements that may be to the right of the insert index
	for(int i = INTARRAYLEAFSIZE - 1; i > insertIndex; i--){
          leaf->keyArray[i] = leaf->keyArray[i-1];
	  leaf->ridArray[i] = leaf->ridArray[i-1];
	}

	//Insert key,rid
	leaf->keyArray[insertIndex] = *((int*)key);
	leaf->ridArray[insertIndex] = rid;
	currentPageNum = 0;
	bufMgr->unPinPage(file, pageNum, true);
	return true;
      }
      bufMgr->unPinPage(file, pageNum, false);
      return false;

}

void BTreeIndex::splitLeaf(LeafNodeInt *child, PageId childNo, const void *key, const RecordId rid, int &propKey, PageId &propPageNo){
 
  //Splits node at child level into two
   int mid = (int)(INTARRAYLEAFSIZE/2); //mid index
   int propogateKey = child->keyArray[mid];

   //Create new leaf
   PageId sibPageNo; Page *sibPage;
   bufMgr->allocPage(file, sibPageNo, sibPage);
   LeafNodeInt *sibLeaf = (struct LeafNodeInt*)sibPage;
   
   //set to max
   for(int i = 0; i < INTARRAYLEAFSIZE; i++){
     sibLeaf->keyArray[i] = INT_MAX;
   }
   //Copy over data
   for(int i = mid, j = 0; i < INTARRAYLEAFSIZE; i++,j++){
     sibLeaf->keyArray[j] = child->keyArray[i];
     sibLeaf->ridArray[j] = child->ridArray[i];
     child->keyArray[i] = INT_MAX;
   }
   sibLeaf->rightSibPageNo = child->rightSibPageNo;
   child->rightSibPageNo = sibPageNo;

   //Insert the key and rid, now that the two pages have space
   if(*((int*)key) < propogateKey){
     insertToLeaf(childNo, key, rid);
   }else{
     insertToLeaf(sibPageNo, key, rid);
   }  
   bufMgr->unPinPage(file, sibPageNo, true);
   propPageNo = sibPageNo;
   propKey = propogateKey;
}

void BTreeIndex::splitNonLeaf(NonLeafNodeInt *child, const void *key, PageId pageNo, int &propKey, PageId &propPageNo, bool root){
  int mid = (int)(INTARRAYNONLEAFSIZE/2);
  int propogateKey = child->keyArray[mid];

  //Create new node
  PageId sibPageNo; Page *sibPage;
  bufMgr->allocPage(file, sibPageNo, sibPage);
  NonLeafNodeInt *sibNode = (struct NonLeafNodeInt*)sibPage;

  //set to max
   for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
     sibNode->keyArray[i] = INT_MAX;
   }
  //Copy data to new node
  for(int i = mid, j = 0; i < INTARRAYNONLEAFSIZE; i++, j++){
    sibNode->keyArray[j] = child->keyArray[i];
    sibNode->pageNoArray[j] = child->pageNoArray[i];
    sibNode->pageNoArray[j+1] = child->pageNoArray[i+1];
    child->keyArray[i] = INT_MAX;
  }
  sibNode->level = child->level; 

  if(*((int*)key) < propogateKey){
   insertNonLeaf(child, key, pageNo);
   
  }else{
   insertNonLeaf(sibNode, key, pageNo);
  }

  
  bufMgr->unPinPage(file, sibPageNo, true);
  
  //add a root page if the root is splitting.
  if(root){
    Page *rootP; PageId rootNo;
    bufMgr->allocPage(file, rootNo, rootP);
    NonLeafNodeInt *root = (struct NonLeafNodeInt*)rootP;

    //set to max
    for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
       root->keyArray[i] = INT_MAX;
    }
    //add the prop key and pages
    root->keyArray[0] = propogateKey;
    root->pageNoArray[0] = rootPageNum;
    root->pageNoArray[1] = sibPageNo;
    root->level = 0;
    rootPageNum = rootNo;
    bufMgr->unPinPage(file, rootNo, true);
  }
  propKey = propogateKey;
  propPageNo = sibPageNo;
}

void BTreeIndex::insertNonLeaf(NonLeafNodeInt *node, const void *key, PageId pageNo){
  //Find free slot
  int selectIndex;
  for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
    if(*((int*)key) < node->keyArray[i]){
      selectIndex = i;
      break;
    }
  }

  //Shift data
  for(int i = INTARRAYNONLEAFSIZE-1; i > selectIndex; i--){
    node->keyArray[i] = node->keyArray[i-1];
    node->pageNoArray[i+1] = node->pageNoArray[i];
  }

  //insert key, page
  node->keyArray[selectIndex] = *((int*)key);
  node->pageNoArray[selectIndex+1] = pageNo;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  std::cout<<"startScan:"<<scanExecuting<<"\n"<<std::flush;
  //End any scan if one is occuring
  if(scanExecuting) endScan();

  lowValInt = *((int*)lowValParm);
  highValInt = *((int*)highValParm);
  
  lowOp = lowOpParm;
  highOp = highOpParm;

  //Verify paramaters
  if(lowValInt > highValInt){
    throw BadScanrangeException();
  }
 
  if(lowOp == LT || lowOp == LTE){
    throw BadOpcodesException();
  }
  
  if(highOp == GT || highOp == GTE){
    throw BadOpcodesException();
  }
 
  if(currentPageNum == 0) currentPageNum = rootPageNum;

  //Scan current node
  Page *page;
  bufMgr->readPage(file, currentPageNum, page);
  bufMgr->unPinPage(file, currentPageNum, false);
  NonLeafNodeInt *node = (struct NonLeafNodeInt*)page;

  //Find the next page to recurse onto
  int childPageNo = 0;
  for(int i = 0; i < INTARRAYNONLEAFSIZE; i++){
    if(lowValInt < node->keyArray[i]){
      childPageNo = node->pageNoArray[i];
      break;
    }
  }
  if(childPageNo == 0) childPageNo = node->pageNoArray[INTARRAYNONLEAFSIZE];

  //Return if leaf otherwise continue
  currentPageNum = childPageNo;
  if(node->level == 1){
    bufMgr->readPage(file, currentPageNum, currentPageData);
    LeafNodeInt *leaf = (struct LeafNodeInt*)currentPageData;

    //Verify leaf contains key
    for(int i = 0; i < INTARRAYLEAFSIZE; i++){

      //if current leaf is empty, check the next leaf to see if it has valid values
      if(leaf->keyArray[i] == INT_MAX){
        if(leaf->rightSibPageNo == 0){
	  break;
        }else{
	  i = 0;
          bufMgr->unPinPage(file, currentPageNum, false);
	  currentPageNum = leaf->rightSibPageNo;
	  bufMgr->readPage(file, currentPageNum, currentPageData);
	  leaf = (struct LeafNodeInt*)currentPageData;
        }
      }
     
      if(verifyKey(leaf->keyArray[i])){
        scanExecuting = true;
	nextEntry = i;
	return;
      }

      //If value is greater than current high op and not empty val(int max) scanning can terminate
      if(leaf->keyArray[i] > highValInt && leaf->keyArray[i] != INT_MAX) break;
    }
    
    //Btree does not contain any valid keys
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = 0;
    throw NoSuchKeyFoundException();
  }else{
    startScan(lowValParm, lowOpParm, highValParm, highOpParm);
  }
}

bool BTreeIndex::verifyKey(int key){
  if(lowOp == GT && highOp == LT){
    return (key > lowValInt && key < highValInt);
  }else if(lowOp == GT && highOp == LTE){
    return (key > lowValInt && key <= highValInt);
  }else if (lowOp == GTE && highOp == LT){
    return (key >= lowValInt && key < highValInt);
  }else{
    return (key >= lowValInt && key <= highValInt);
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
  if(!scanExecuting){
    throw ScanNotInitializedException();
  }

  if(nextEntry == -1){ //Case where no more right sibling page exists
    throw IndexScanCompletedException();
  }
  
  //verify current key is within paramaters
  LeafNodeInt *leaf = (struct LeafNodeInt*)currentPageData;
  if(verifyKey(leaf->keyArray[nextEntry])){
    outRid = leaf->ridArray[nextEntry];		
  }else{
    throw IndexScanCompletedException();
  }
 
  //Increment next entry
  if(nextEntry < INTARRAYLEAFSIZE - 1){
    nextEntry++;
  }else{
    //Case where end of full page has been reached.
    //Current page is unpinned and next page is read in, if it exists.
    if(leaf->rightSibPageNo != 0){
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = leaf->rightSibPageNo;
      bufMgr->readPage(file, currentPageNum, currentPageData);
      nextEntry = 0;
    }else{
      nextEntry = -1; //mark nextentry invalid for next iteration
    }
  }

  //Increments page in case end of a partially filled page is encountered
  while(leaf->keyArray[nextEntry] == INT_MAX){
    if(leaf->rightSibPageNo != 0){
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = leaf->rightSibPageNo;
      bufMgr->readPage(file, currentPageNum, currentPageData);
      nextEntry = 0;
      leaf = (struct LeafNodeInt*)currentPageData;
    }else{
      nextEntry = -1;
      break;
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
   std::cout<<"endScan:"<<scanExecuting<<"\n"<<std::flush;
  //No scan running
  if(!scanExecuting){
    throw ScanNotInitializedException();
  } 

  //Perform cleanup
  bufMgr->unPinPage(file, currentPageNum, false);
  currentPageNum = 0;  
  scanExecuting = false;
}

}
