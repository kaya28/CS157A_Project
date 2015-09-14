#include "stdafx.h"
#include "TokenizerUtils.h"
#include "DataStorage.h"

#define WIN32_LEAN_AND_MEAN 


//Each thread instance will run its own instance of this function.
DWORD CThreadManager::ThreadProc() 
{
	static int nextThreadID=0;
	int threadID=++nextThreadID;
	printf("ThreadProc(ThreadID=%d) started\n",threadID);
	
	HANDLE events[]={mEndEvent, mTaskAvailEvent};

	long fileIndex;
	bool fileParsed;
	while (TRUE)
	{
		DWORD waitNum=WaitForMultipleObjects(2,events,FALSE, INFINITE);
		//printf("ThreadProc(ID=%d) WaitForMultiple returned %d\n",threadID, waitNum);
		if (waitNum==WAIT_OBJECT_0)
		{
			//printf("ThreadProc(ID=%d) received mEndEvent\n",threadID);
			break; // app ending
		}

		while (TRUE)
		{
			{				
				CSLock cslock(mCSFileListIndex); // Get access to the file index

				if (mCurrFileListIndex >= (long) mInputFileList->size())  
				{	// The last thread to finish the list should indicate no more files left.
					//printf("ThreadProc(ID=%d) ResetEvent(mTaskAvailEvent);\n",threadID);
					ResetEvent(mTaskAvailEvent);
					break; // Done, no more. Note this 'If' check is needed in case of race condition
				}

				fileIndex = mCurrFileListIndex++;
			}

			//printf("ThreadProc(ID=%d) parseFile(%s)\n",threadID, mInputFileList->at(fileIndex).c_str());
			fileParsed = parseFile(mInputFileList->at(fileIndex));

			//fileParsed = parseFile(mInputFileList->at(fileIndex).c_str());

			{
				CSLock cslock(mCSFileListIndex);
				mCurrFilesProcessed++;  // whether succeeded or not, we have consumed this file

				if (fileParsed && mCurrTargetTupleCount==1)
					mRunningDocCount++;  //update count only if we actually parsed it
			}
		}
	
		//printf("ThreadProc(ID=%d) task done, SetEvent(mTaskCompletedEvent)\n",threadID);
		SetEvent(mTaskCompletedEvent);
	}

	printf("ThreadProc(ThreadID=%d) completed\n",threadID);
	return 0;
}


bool CThreadManager::mergeTupleList(long targetTupleCount, CFreqList& freqTuples)
{
	CSLock cslock(mCSGuardTupleLists);

	map<string, CToken>& mapList = freqTuples.getList();
	map<string, CToken>::iterator it;
	for(it=mapList.begin(); it != mapList.end(); it++)
	{
		CToken& t = (it->second);
		mTupleLists[targetTupleCount].mergeToken(t, mNextConcept_DBID);
	}

	return true;
}


DWORD WINAPI CThreadManager::ThreadLaunch( LPVOID lpParam ) 
{
	CThreadManager *theInstance = (CThreadManager*) lpParam;
	return theInstance->ThreadProc();
}

// Return true if file was parsed, false if file was already parsed before.
bool CThreadManager::parseFile(CURLPage& thePage)
{//tokenizer
	bool bParsed = true;
	try 
	{
		if (!QueryMode() && thePage.Ignore())
			return false;

		//printf(".");
	
		// if mCurrTargetTupleCount==1, should check DB to determine if this has been run before

		// Check if file already scanned before, if so then ignore it.
		//ADODB::_RecordsetPtr pRS=NULL;
		//string csSQL = "exec sp_IsURLScanned '";
		//csSQL += docName + "'";
		//if (mDB->ExecuteSQL_IntoRecordset(pRS, csSQL))
		//{
		//	long isExist = (long) pRS->Fields->Item["isExist"]->Value;
		//	bParsed = (isExist>0);
		//	pRS->Close();
		//	pRS = NULL;
		//}

		P267_FILE *fp = P267_open(thePage.Filename().c_str(), P267_IOREAD);//open the file
		//
		if (fp != NULL)
		{
			CFreqList tempTupleList;
			tempTupleList.QueryMode(QueryMode());

			deque<CToken> lsTokens;
			CToken aToken;//store token info
			string sTemp;// store a word in sentence

			int c,d=10;
			int wc = 0;

			//find file size
			//fseek(fp, 0, SEEK_END); // seek to end of file 
			//long filesize = ftell(fp); // get current file pointer 
			//fseek(fp, 0, SEEK_SET); // seek back to beginning of file 
			//printf("\nparseFile(%s) with %d bytes....       \n",docName.c_str(), filesize);

			// ********** Put previous data into DB
			//string csSQL;
			//char ss[512];
			//do
			//{
			//	csSQL="";
			//	sTemp="";
			//	while((c = P267_getcf(fp)) != P267_ENDFILE && (c!=P267_NEWLINE) && (c!=P267_CARRIAGERETURN) ) 
			//		sTemp += c;

			//	sprintf(ss, "exec sp_UpdateConceptByToken %ld,'%s', 5", mNextConcept_DBID++, sTemp.c_str());
			//	mNextConcept_DBID++;
			//	csSQL += ss;
			//	try
			//	{
			//		mDB->ExecuteSQL(csSQL);
			//	}
			//	catch(...)
			//	{
			//		printf("** Error executing: %s\n", csSQL.c_str());
			//	}
			//} while (c != P267_ENDFILE);
			//return true;

			// assume the first line is the HTTP URL
			while((c = P267_getcf(fp)) != P267_ENDFILE && (c!=P267_NEWLINE) && (c!=P267_CARRIAGERETURN) &&  (c!=P267_HTMLDELIMITER) ) 
				sTemp += c;//add a word into stem until one of endfile or new line or carrigereturn or htmoldelimiter comes

			// if there were no filename in the first line, then use the name that opened the file
			if (sTemp.find("http") != 0)
				thePage.URL(thePage.Filename());//get file
			else
				thePage.URL(sTemp);//access to web
	
			bool usAddressDetected=false;
			//no need for this P267_ungetc(c,fp);
			while((c = P267_getcf(fp)) != P267_ENDFILE) 
			{
				if (!isLegalChar(c) && (c!=P267_NEWLINE) && (c!=P267_CARRIAGERETURN))
				{//check if c is legal char(A-Z,a-z,0-9, /)
					wc++;
					//fprintf(ofp," [0x%x]\t%d\t unprintable\n", c, wc);
				}
		
				if (isLegalChar(c))
				{ 
					aToken.clear(); //clear 
					aToken.LastDocFound(thePage.Filename().c_str());// store file namwe
					sTemp.erase();

					//Form a generic token with all legal chars
					do 
					{
						sTemp += c;
						//fprintf(ofp,"%c", c);
					} 
					while (isLegalChar(c=P267_getcf(fp)));
					P267_ungetc(c,fp);
					wc++;
					//fprintf(ofp,"\t%d\t%s\n",wc,srcfilename);
			
					aToken.set(sTemp.c_str(), sTemp.c_str(), wc);//set first letter of token

					////// Determine whether the period character signifies end of sentence.
					//if (aToken.Token()[aToken.Token().length()-1] == '.' || aToken.Token()[aToken.Token().length()-1] == ',')
					//{
					//	//period is last char, remove it.
					//	string s = aToken.Token().substr(0,aToken.Token().length()-1);
					//	aToken.set(s.c_str(), aToken.Pos());

					//	wc++; // this accounts for the extra period just removed.
					//}
			
					if (aToken.Token().empty())
						continue; // it is blank after trimming so skip it

					aToken.StemIt(); // convert to its stem
					//stem for a token
					/// Insert into our token list
					lsTokens.push_back(aToken);

					/// **** Disable address detection, not important for now. 5/2/12
					/////// Determine if possible US address and put it into a single token
					//if (isUSState(aToken.Token()))
					//{	
					//	usAddressDetected=true;  // allow one more iteration to consume the zip code.
					//} 
					//else if (usAddressDetected && isZipCode(aToken.Token()))
					//{
					//	// Assume we have a zip code. This is a US address, so merge all relevant tokens into one.
					//	mergeUSAddress(lsTokens, wc);
					//	usAddressDetected=false;
					//} 
					//else 
					//{
					//	usAddressDetected=false; //false alarm, not US address
					//}
				} 
				else
				{
					//we try to analyze when there is newline, but it has to be greater than minsize but less than maxsize
	
					if ((lsTokens.size() >= MAXPARAGRAPH_SIZE) || 
						( ((c==P267_NEWLINE) || (c==P267_CARRIAGERETURN)) && (lsTokens.size() > MINPARAGRAPH_SIZE)) )
					{
						// permute within the paragraph
						long currPos=ftell(fp);
						//printf("\b\b\b\b\b\b\b\b[%05.2lf%%]",currPos*100.0/filesize);
						//printf("parseFile(%s) [%05.2lf%%]\n",docName.c_str(), currPos*100.0/filesize);
						permuteParagraph(mCurrTargetTupleCount, thePage, lsTokens, tempTupleList);

						//mergeTupleList(mCurrTargetTupleCount, tempTupleList);
						mergeTokenList(lsTokens);
						lsTokens.clear();

						////// Output to file 
						//outputToFile(ofp, srcfilename, lsTokens);
					}		
				}

			}

			//permute and output remaining
			permuteParagraph(mCurrTargetTupleCount, thePage, lsTokens, tempTupleList);

			//tempTupleList.pruneTermFreq(0.01, 0.3);
			tempTupleList.pruneTermFreq(TERMFREQ_FROM, TERMFREQ_TO);

			mergeTupleList(mCurrTargetTupleCount, tempTupleList);
			tempTupleList.clear();

			// for list all tokens
			mergeTokenList(lsTokens);//merge all thread
			lsTokens.clear();


			P267_close(fp);
		}
		else
		{
			printf("Can't open %s\n", thePage.Filename().c_str());
		}
	}
	catch(...)
	{
		printf("*** Exception in parseFile %s\n", thePage.Filename().c_str());
	}

	return bParsed;
}

// targetTupleCount: the n-tuple we're trying to find.
bool CThreadManager::permuteParagraph(long targetTupleCount, CURLPage& pPage, deque<CToken>& lsTokens, CFreqList& freqTuples)
{
	static char PROGRESS[3] = { '/', '-', '\\' };

	CToken aToken;

	for (long i=0; i < (long) lsTokens.size(); i++)
	{
//		printf("\b%c", PROGRESS[i%3]); // visual displays progress
		aToken = lsTokens[i];

		if ((aToken.Token().length() > 100) || isStopWord(aToken.Token(), 1))
			continue;

		if (targetTupleCount==1) {
			freqTuples.LogToken(aToken, pPage);
		} else {
			// mTupleList[minus one] is stable so no need to acquire lock first in order to check it.
			//if (mTupleLists[1].exists(aToken.Key()))
			if (mTupleLists[1].DocFreq(aToken.Key()) >= DOCFREQMINCOUNT)
			{   // only permute if (n-1)-tuple exists 
				permuteParagraph_r(2, targetTupleCount, aToken, i+1, MIN(i+WINDOW_SIZE, lsTokens.size()), pPage, lsTokens, freqTuples);
			}
		}
	}

	return true;
}

// This function is recursively called starting with 2-tuple and on.
// targetTupleCount: the target n-tuple. 
// nDepth: this starts at 2 and goes up til targetTupleCount.  When nDepth == TargetTupleCount, that is when recursion terminates.
bool CThreadManager::permuteParagraph_r(long nDepth, long targetTupleCount, CToken& pTokenPrefix, long nStartIdx, long nEndIdx, CURLPage& pPage, deque<CToken>& lsTokens, CFreqList& freqTuples)
{
	CToken aNewToken, aNewTokenTuple;
	//char sTemp[256];
	string csTemp, csTemp2;
	long i, toIdx;

	try {
		/* // Debug print
		printf("permuteParagraph(");
		for (i=nStartIdx; i < toIdx; i++)
		{
			aToken = lsTokens[i];
			printf("%s ",aToken.getToken());
		}
		printf(")\n%d\n\n", toIdx-nStartIdx);
		*/
	
		for (i=nStartIdx; i < nEndIdx; i++)
		{
			aNewToken = lsTokens[i];
			//if (!mTupleLists[1].exists(aToken.Token()))
			//{   
			//	continue; // only permute if second token has not been pruned AND it is not doubled
			//}

			if (isStopWord(aNewToken.Token(), 1))
				continue;

			// disallow repetitive words [*** note this also cancels override! ]
			if (pTokenPrefix.Token().find(aNewToken.getToken()) != string::npos)
				continue;

			//sprintf(sTemp, "%s %s", sPrefix, aToken.getToken());
			csTemp = pTokenPrefix.Token();
			csTemp += " ";
			csTemp += aNewToken.getToken();

			csTemp2 = pTokenPrefix.getTokenOrig();
			csTemp2 += " ";
			csTemp2 += aNewToken.getTokenOrig();

			aNewTokenTuple.clear();
			aNewTokenTuple.LastDocFound(pPage.URL());
			aNewTokenTuple.set(csTemp.c_str(), csTemp2.c_str(), pTokenPrefix.FirstPos());
			aNewTokenTuple.Distance(aNewToken.FirstPos()-pTokenPrefix.FirstPos());

			//if (nDepth == targetTupleCount)
			//	freqTuples.LogToken(aNewTokenTuple);
			//else
			//	permuteParagraph_r(nDepth+1,targetTupleCount, nToken1Pos, sTemp, i+1, nEndIdx, sSrcfilename, lsTokens, freqTuples);

			if (nDepth == targetTupleCount) {
				freqTuples.LogToken(aNewTokenTuple, pPage);
			} else {
				// mTupleList[minus one] is stable so no need to acquire lock first in order to check it.
				//if (mTupleLists[nDepth].exists(aNewTokenTuple.Key()) )
				if (mTupleLists[nDepth].DocFreq(aNewTokenTuple.Key()) >= DOCFREQMINCOUNT)
				{   // only permute if (n-1)-tuple exists and DocFreq was more than 1
					permuteParagraph_r(nDepth+1, targetTupleCount, aNewTokenTuple, i+1, MIN(i+WINDOW_SIZE, lsTokens.size()), pPage, lsTokens, freqTuples);
				}
			}
		}
	}
	catch(...)
	{
		printf("*** Exception in CThreadManager::permuteParagraph_r()\n");
	}

	return true;
}

//output the first count of items to file.
//if there is any token to be output has an ending comma, the comma will be removed.  This was 
//an artifact in helping parse US address.
void CThreadManager::outputToFile(P267_FILE *ofp, char* filename, deque<CToken>& lsTokens)
{
	CToken aToken;
	deque<CToken>::const_iterator ii;
	char sSQL[1000];

	for(ii=lsTokens.begin(); ii<lsTokens.end(); ii++)
	{
		aToken = lsTokens.front();

		//throw away the token if it only contains the comma
		if (aToken.Token()!=",")
		{
			if (aToken.Token()[aToken.Token().length()-1] == ',') {
				string s = aToken.Token().substr(0,aToken.Token().length()-1);
				//nowrite fprintf(ofp,"\"%s\",%s,%d\n",s.c_str(), filename, aToken.getPos());
			} 
			else {
				//nowrite fprintf(ofp,"\"%s\", %s,%d\n",aToken.getToken(),filename,aToken.getPos());
				sprintf_s(sSQL, "exec sp_UpdateConcept %d, %s, %d, %d, %d, %d", aToken.DBID(), aToken.getToken(), 1, aToken.Freq(), aToken.DocFreq(),0); 
				string csSQL = sSQL;
				if (!mDB->ExecuteSQL(csSQL))
				{
					printf("** Error exec sp_UpdateConcept: %s\n", sSQL);
				}
			}
		}

		lsTokens.pop_front();
	}
}

bool CThreadManager::parseNtuple(long targetTuple, const string& sFilePrefix)
{
	printf("\n***** parseNtuple(analyzing %d-tuples) started *****\n",targetTuple);
	bool brc=true;
	
	bool writeToFileDone=false;

	{
		CSLock cslock(mCSFileListIndex);
		mCurrFileListIndex = 0;
		mCurrFilesProcessed = 0;
		mCurrTargetTupleCount = targetTuple;
	}

	SetEvent(mTaskAvailEvent);
	ResetEvent(mTaskCompletedEvent);

	//long completionCountdown =mThreadCount;

	long iProgress=0;
	while(true)
	{
		if (!writeToFileDone)
		{
			//use this cpu cycle to save to DB/file of previous tuple count data
			writeToFileDone=true;
			if (mCurrTargetTupleCount>=2)
			{
				closeMasterTokensFile();

				printf("\b\b\b\b\b\b\b\b\b\b\b[%05.2lf%%][%c]", mCurrFilesProcessed*100.0/mInputFileList->size(), PROGRESS[iProgress%3]);
				Serialize(mCurrTargetTupleCount-1, sFilePrefix);
			}
		}

		iProgress++;
		//{
		//	printf("parseNtuple waited mTaskCompletedEvent a Thread has completed\n");
		//	completionCountdown--;
		//}
		//printf("parseNtuple waited mTaskCompletedEvent completionCountdown=%d\n",completionCountdown);

		WaitForSingleObject(mTaskCompletedEvent, 500);
		{
			CSLock cslock(mCSFileListIndex);
			if (mCurrFilesProcessed >= (long) mInputFileList->size())  
			{
				printf("\b\b\b\b\b\b\b\b\b\b\b[100%%][-]");
				break; // mCurrTargetTupleCount task is done!
			} 
			//printf("parseNtuple: mCurrFileListIndex=%d\n",mCurrFileListIndex);
			//printf("[%05.2lf%%]", mCurrFilesProcessed/(double) mInputFileList->size());
			printf("\b\b\b\b\b\b\b\b\b\b\b[%05.2lf%%][%c]", mCurrFilesProcessed*100.0/mInputFileList->size(), PROGRESS[iProgress%3]);
		}

	}

	ResetEvent(mTaskAvailEvent);

	pruneNtuple(targetTuple,sFilePrefix);
	printf("\n===== parseNtuple(analyzing %d-tuples) completed =====\n",targetTuple);

	return brc;
}

bool CThreadManager::pruneNtuple(long targetTuple, const string& sFilePrefix)
{
	CSLock cslock(mCSGuardTupleLists);
	try
	{
		P267_FILE *fpOut;
		char ss[512];

		sprintf_s(ss,"%s.%dtuple.pruneddoc.txt",sFilePrefix.c_str(),targetTuple);
		printf("\nPruning %d-tuples to file '%s' ...\n", targetTuple, ss);
		fpOut = P267_open(ss, P267_IOWRITE);

		mTupleLists[targetTuple].pruneDocFreq(fpOut, DOCFREQ_FROM, DOCFREQ_TO, mActualSessionDocCount);
		//tokenize document......?

		if (fpOut)
			P267_close(fpOut);

		//mTupleLists[targetTuple].pruneDocFreq(DOCFREQ_FROM, DOCFREQ_TO, (long) mRunningDocCount);

		//if (n==1)
		//	mTupleLists[n].pruneDocFreq(0.1, 0.5, (long) mInputFileList->size());
		//else 
			//mTupleLists[n].pruneDocFreq(0.003, 0.5, (long) mInputFileList->size());	
	}
	catch(...)
	{
		printf("*** Exception in CThreadManager::pruneNtuple targetTuple=%d\n",targetTuple);
	}

	return true;
}

CThreadManager::CThreadManager()
{ 
	InitializeCriticalSection(&mCSFileListIndex); 
	InitializeCriticalSection(&mCSGuardTupleLists);
	InitializeCriticalSection(&mCSGuardMasterList);

	mTaskAvailEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
	mEndEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
	mTaskCompletedEvent = CreateEvent(NULL, FALSE, FALSE, NULL);

	mMasterTokenCount = 0;
	mRunningDocCount = 0;
	mQueryMode = false;
	mActualSessionDocCount = 0;
}

// This function will remove all the files that have already been previously scanned.
// This will also assign unique DBID. 
void CThreadManager::SetInputFileList(std::deque<CURLPage>* fl)
{
	try
	{
		mActualSessionDocCount = 0;
		mInputFileList = fl;

		std::deque<long> removeList;

		ADODB::_RecordsetPtr pRS=NULL;
		string csSQLCmd = "exec sp_IsURLScanned ?";
		//string csSQL;

		int i=0;
		bool exitLoop=false;
		std::deque<CURLPage>::iterator itPage = mInputFileList->begin();
		//std::deque<CURLPage>::iterator itPage2;
		while (itPage != mInputFileList->end())
		{
			i++;
			CURLPage& thePage = *itPage;
			//itPage2 = mInputFileList->end();

			// Check if file already scanned before, if so then ignore it.
			//csSQL = csSQLCmd + thePage.URL() + "'";
			if (mDB->ExecuteSP_IntoRecordset(pRS, csSQLCmd, thePage.URL()))
			{
				long isExist = (long) pRS->Fields->Item["isExist"]->Value;
				if (isExist > 0)
				{
					// This file was already scanned in previous run, dump it
					itPage->Ignore(true);
					//itPage2 = itPage;
					//mInputFileList->erase(itPage2);
				}
				else
				{
					// new file, assign DBID to it
					thePage.ID(mNextURL_DBID++);

					mActualSessionDocCount++;
				}
				pRS->Close();
				pRS = NULL;
			} 
			else
			{
				// new file, assign DBID to it
				thePage.ID(mNextURL_DBID++);

				mActualSessionDocCount++;
			}

			itPage++;

			// This temp var to flag us to exit the loop is necessary because of the way iterator will lose its place
			if (itPage ==mInputFileList->end())
				exitLoop=true;

			//if (itPage2 != mInputFileList->end())
				//mInputFileList->erase(itPage2);

			if (exitLoop)
				break;
		}
	}
	catch(...)
	{
		printf("\n\n***** EXCEPTION in CThreadManager::SetInputFileList *** \n\n");
	}


}


CThreadManager::~CThreadManager()
{
	DeleteCriticalSection(&mCSFileListIndex);
	DeleteCriticalSection(&mCSGuardTupleLists);
	DeleteCriticalSection(&mCSGuardMasterList);
	CloseHandle(mTaskAvailEvent);
	CloseHandle(mEndEvent);
	CloseHandle(mTaskCompletedEvent);

}


void CThreadManager::InitThreads(int threadcount)
{
	mThreadCount = threadcount;
	mThreadHandles = new HANDLE[mThreadCount];

	for (int i=0; i<mThreadCount; i++)
	{
		mThreadHandles[i] = CreateThread(NULL, 0, ThreadLaunch, this, 0, NULL);
	}
}


void CThreadManager::EndThreads()
{
	SetEvent(mEndEvent);
}

// form the address into a single token
bool CThreadManager::mergeUSAddress(deque<CToken>& lsTokens, int& wc)
{
	bool bMerged=false;
	//step back until number or max of 20 tokens

	int max=20;
	CToken aToken;
	deque<CToken>::iterator ii;
	ii=lsTokens.end();
	ii--; //this is the zip code
	while (ii!=lsTokens.begin() && max-- > 0)
	{
		ii--;
		if (isNumber(ii->Token()))
		{
			//start concatenate from here
			int tokenCount=0;
			deque<CToken>::iterator starterase=ii;
			string aUSAddress;
			while (ii!=lsTokens.end())
			{
				aUSAddress += ii->Token();
				aUSAddress += " ";
				tokenCount++;
				ii++;
			}

			wc -= tokenCount;

			while (tokenCount-- > 0)
			{
				lsTokens.pop_back();
			}

			aToken.set(aUSAddress.c_str(),aUSAddress.c_str(),wc);
			lsTokens.push_back(aToken);
			
			break;
		}

	}

	bMerged=true;
	return bMerged;
}

// Output 1 tuple file
void CThreadManager::Serialize(long nTupleIndex, const string& outfilePrefix)
{
	P267_FILE *fpOut;
	char ss[512];

	//Shouldn't need this lock. CSLock cslock2(mCSGuardTupleLists);
	sprintf_s(ss,"%s.%dtuple.txt",outfilePrefix.c_str(),nTupleIndex);
	printf("\nWriting %d-tuples to file '%s' ...\n", nTupleIndex, ss);
	if ((fpOut = P267_open(ss, P267_IOWRITE))!=NULL)
	{
		mTupleLists[nTupleIndex].Serialize(fpOut, mDB, nTupleIndex, mMasterTokenCount, mRunningDocCount );
		P267_close(fpOut);
	}

	if (!QueryMode())
	{
		if (nTupleIndex==1) 
		{
			// we're actually writing TargetTupleCount==1
			// Save the running file count and the files that were scanned

			sprintf_s(ss, "exec sp_UpdateRunningDocCount %d", mRunningDocCount); 
			string csSQL = ss;
			if (!mDB->ExecuteSQL(csSQL))
			{
				printf("** Error executing: %s\n", ss);
			}

			//std::deque<CURLPage>::iterator itUrl = mInputFileList->begin();
			//while (itUrl != mInputFileList->end())

			long iProgress=0;
			for (long ii=0; ii < mInputFileList->size(); ii++)
			{
				//display progress indicator
				if ((ii%100)==0) 
				{
					printf("\b\b\b\b\b\b\b\b\b\b\b[%05.2lf%%][%c]", mCurrFilesProcessed*100.0/mInputFileList->size(), PROGRESS[iProgress%3]);
					iProgress++;

				}
			
				CURLPage &pPage = mInputFileList->at(ii);// *itUrl;
			
				if (pPage.Ignore())
					continue;

				//sprintf(ss, "exec sp_InsertURLPage %d, '%s', %d", pPage.ID(), pPage.URL().c_str(), pPage.WordCount()); 
				//string csSQL = ss;
				//if (!mDB->ExecuteSQL(csSQL))

				if (!mDB->sp_InsertURLPage(pPage.URL(), pPage.ID(), pPage.WordCount()))
				{
					printf("** Error calling mDB->sp_InsertURLPage: %s\n", pPage.URL());
				}

				//itUrl++;
			}

		}
	}

	printf("Writing %d-tuples completed.\n", nTupleIndex);
}


void CThreadManager::toFiles(const string& outfilePrefix)
{
	printf("Writing to files...\n");
	P267_FILE *fpOut;
	char sfilename[512];

	//sprintf(sfilename,"%s.tokens.txt",outfilePrefix.c_str());
	//if ((fpOut = P267_open(sfilename, P267_IOWRITE))!=NULL)
	//{
	//	CSLock cslock(mCSGuardMasterList);
	//	deque<CToken>::iterator ii;

	//	fprintf(fpOut,"Total rows: %d\n==========================\n", mMasterTokenList.size());
	//	fprintf(fpOut,"Token     \tDocName     \tPosition\n");
	//	fprintf(fpOut,"--------------------------------------\n");
	//	for(ii= mMasterTokenList.begin(); ii<mMasterTokenList.end(); ii++)
	//	{ 
	//		fprintf(fpOut,"\"%s\", %s, %d\n", ii->Token().c_str(), "null", ii->getPos());
	//	}
	//	P267_close(fpOut);
	//}

	CSLock cslock2(mCSGuardTupleLists);
	for (int i=1; i <= 6; i++)
	{
		sprintf_s(sfilename,"%s.%dtuple.txt",outfilePrefix.c_str(),i);
		if ((fpOut = P267_open(sfilename, P267_IOWRITE))!=NULL)
		{
			mTupleLists[i].Serialize(fpOut, mDB, i, mMasterTokenCount, (long) mInputFileList->size());
			P267_close(fpOut);
		}
	}
}

// Only do merge when mCurrTargetTupleCount==1
bool CThreadManager::mergeTokenList(deque<CToken>& lsTokens)
{
	if (mCurrTargetTupleCount!=1 || mfpMasterTokens==NULL)
		return false;

	CSLock cslock(mCSGuardMasterList);
	deque<CToken>::iterator ii;

	for(ii=lsTokens.begin(); ii<lsTokens.end(); ii++)
	{
		//nowrite fprintf(mfpMasterTokens,"%-30s   %-50s   %-10d\n", ii->Token().c_str(), ii->LastDocFound().c_str(), ii->getPos());
		mMasterTokenCount++;
		//mMasterTokenList.push_back(*ii);
	}
	fprintf(mfpMasterTokens,"<p>\n");

	return true;
}


void CThreadManager::openMasterTokensFile(const string& outfilePrefix)
{
	char sfilename[512];

	sprintf_s(sfilename,"%s.tokens.txt",outfilePrefix.c_str());
	if ((mfpMasterTokens = P267_open(sfilename, P267_IOWRITE))!=NULL)
	{
		//fprintf(mfpMasterTokens,"Token     \tDocName     \tPosition\n");
		fprintf(mfpMasterTokens,"%-30s   %-50s   %-10s\n", "Token", "DocName", "Position");
		fprintf(mfpMasterTokens,"----------------------------------------------------------------------------------------------\n");
	}
}

void CThreadManager::closeMasterTokensFile()
{
	if (mfpMasterTokens)
	{	fprintf(mfpMasterTokens,"Total token count: %d\n", mMasterTokenCount);
		P267_close(mfpMasterTokens);
	}
	mfpMasterTokens=NULL;
}

// Prune it. This is a DB stored proc call, logic of when to prune is controlled in the DB stored proc.
void CThreadManager::pruneDBDocFrequency()
{
	try
	{
		printf("CThreadManager::pruneDBDocFrequency()\n");

		ADODB::_RecordsetPtr pRS=NULL;
		//string csSQL = "exec sp_PruneByDocFrequency";
		string csSQL = "exec sp_PruneTokensAll";
		printf("%s...\n", csSQL.c_str());
		if (!mDB->ExecuteSQL(csSQL, 300))
			printf("*** Exception Error CThreadManager::pruneDBDocFrequency %s\n", csSQL.c_str());

		char sCmd[256];
		sprintf(sCmd, "exec sp_PruneDocFreqLessThan %d", DOCFREQMINCOUNT);
		csSQL = sCmd;
		printf("%s...\n", csSQL.c_str());
		if (!mDB->ExecuteSQL(csSQL, 300))
			printf("*** Exception Error CThreadManager::pruneDBDocFrequency %s\n", csSQL.c_str());
	}
	catch(...)
	{
		printf("\n\n***** EXCEPTION in CThreadManager::pruneDBDocFrequency *** \n\n");
	}
}

void CThreadManager::InitFromDB(CDataStorage *pdb) 
{ 
	printf("Initializing DB ...");
	CSLock cslock(mCSGuardTupleLists);

	mDB=pdb; 
	ADODB::_RecordsetPtr pRS=NULL;
	char sSQL[1000];
	CToken aToken;
	CComVariant cvTokens, freq, docfreq, neverdel, dbid;

	try
	{
		sprintf_s(sSQL, "exec sp_GetMaxConceptID"); 
		if (mDB->ExecuteSQL_IntoRecordset(pRS, sSQL))
		{
			mNextConcept_DBID = (long) pRS->Fields->Item["maxval"]->Value + 1;
			pRS->Close();
			pRS = NULL;
		}
	
		sprintf_s(sSQL, "exec sp_GetMaxURLPagesID"); 
		if (mDB->ExecuteSQL_IntoRecordset(pRS, sSQL))
		{
			mNextURL_DBID = (long) pRS->Fields->Item["maxval"]->Value + 1;
			pRS->Close();
			pRS = NULL;
		}

		sprintf_s(sSQL, "exec sp_GetConfig"); 
		if (mDB->ExecuteSQL_IntoRecordset(pRS, sSQL))
		{
			mRunningDocCount = (long) pRS->Fields->Item["RunningDocCount"]->Value;
			MAXPARAGRAPH_SIZE = (long) pRS->Fields->Item["ParagraphMax"]->Value;

			DOCFREQ_FROM = (float) pRS->Fields->Item["DocFreqFrom"]->Value;
			DOCFREQ_TO = (float) pRS->Fields->Item["DocFreqTo"]->Value;

			TERMFREQ_FROM = (float) pRS->Fields->Item["TermFreqFrom"]->Value;
			TERMFREQ_TO = (float) pRS->Fields->Item["TermFreqTo"]->Value;

			MINPARAGRAPH_SIZE = (long) pRS->Fields->Item["ParagraphMin"]->Value;

			DOCFREQMINCOUNT = (long) pRS->Fields->Item["DocFreqMinCount"]->Value;

			MAX_TUPLE_COUNT = (long) pRS->Fields->Item["TupleCount"]->Value;

			pRS->Close();
			pRS = NULL;
		}

		sprintf_s(sSQL, "exec sp_GetStopWords"); 
		if (mDB->ExecuteSQL_IntoRecordset(pRS, sSQL))
		{
			while (!pRS->EndOfFile)
			{
				cvTokens = pRS->Fields->Item["StopWord"]->Value;
				mStopWords.insert(map<string, int>::value_type((char*) bstr_t(cvTokens.bstrVal),0));
				pRS->MoveNext();
			}
			pRS->Close();
			pRS = NULL;
		}

		for (int i=1; i<7; i++)
		{
			sprintf_s(sSQL, "exec sp_GetConcepts %d", i); 
			//set SQL command to sSQL variable with int = 1
			if (mDB->ExecuteSQL_IntoRecordset(pRS, sSQL))
			{//ExecuteSQL_IntoRecordset sent a request to SQL server and get response then set to pRS. 

				while (!pRS->EndOfFile)
				{//check pRS is not null(here pRS holds tons of records)
					aToken.clear();
					//walk through the records one by one. 
					aToken.DBID((long)pRS->Fields->Item["ID"]->Value);
				
					cvTokens = pRS->Fields->Item["Tokens"]->Value;//cvToken holds value of Tokens (1st: wall street will be hold)
					aToken.set((char*) bstr_t(cvTokens.bstrVal),0);//Go to Token.cpp and get aToken orign which means stem the word.
				
					aToken.Freq((long)pRS->Fields->Item["Frequency"]->Value);
					aToken.DocFreq((long)pRS->Fields->Item["DocFrequency"]->Value);
					aToken.Perm((int)pRS->Fields->Item["Perm"]->Value);

					aToken.Dirty(false); // these are loaded from DB, this flag will be set if there are changes.
					//create data for a row up to here
					mTupleLists[i].InitAdd(aToken);//save data from DB tempory and locally. Classify by index based on tokenCount

					pRS->MoveNext();//go to the next row.
				}

				pRS->Close();
				pRS = NULL;
			}
		}

		printf("Done\n");
	}
	catch(...)
	{
		printf("\n\n***** EXCEPTION in CThreadManager::InitFromDB *** \n\n");
	}
}

// If stop word, return true
// Any of these conditions are considered stopwords
//	1. in StopWord table
//	2. a number
//	3. a single character
//	4. repeated char 3 times or more
//	5. non-displayable char
bool CThreadManager::isStopWord(const string& theWord, const int pTupleCount)
{
	bool bYes = false;

	try
	{
		bool isAllAlpha=true, hasRepeatedChar=false;
		char sLower[52];
		int i;
		memset(sLower,0,52);
		for (i=0; i < theWord.size() && i<51; i++)
		{
			sLower[i]=tolower(theWord.at(i));
			if ((sLower[i]<65) || !isalpha(sLower[i]))
				isAllAlpha=false;

			if (!hasRepeatedChar && i>1)
			{
				if ((sLower[i]==sLower[i-1]) && (sLower[i]==sLower[i-2]) )
					hasRepeatedChar=true; // having 3 repeated chars will be discarded
			}
		}
		//sLower[i]=0;

		// Being already in DB overrides any rule so do that check first.
		//	if in mTupleLists[] means it was already in DB.
		map<string, CToken>& theList = mTupleLists[pTupleCount].getList();
		map<string, CToken>::iterator it = theList.find(sLower);
		if (it == mTupleLists[pTupleCount].getList().end())
		{
			if (pTupleCount == 1)
			{
				bYes = (hasRepeatedChar || !isAllAlpha || (theWord.size()<=1)); // must be all letters. must be greater than 1 character

				if (!bYes)
				{
					// this checks StopWords table
					map<string, int>::iterator it = mStopWords.find(sLower);
					bYes = (it != mStopWords.end());
				}
			}
		}
	}
	catch(...)
	{
		printf("*** Exception in isStopWord\n");
	}

	return bYes;
}

void CThreadManager::QueryMode(bool f) 
{ 
	mQueryMode=f; 
	for (int i=1; i <= 6; i++)
	{
		mTupleLists[i].QueryMode(f);
	}
}

bool CThreadManager::QueryMode() 
{ return mQueryMode; }
