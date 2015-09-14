using namespace std;

#include "FreqList.h"
#include "URLPage.h"

class CFreqList;
class CToken;
class CDataStorage;

enum TaskStatus { Started, InProgress, Completed, Canceled, Error };
static char PROGRESS[3] = { '/', '-', '\\' };

//Base class for tasks
class CThreadManager
{
	//database
	CDataStorage *mDB;
	long mNextConcept_DBID;
	long mNextURL_DBID;

	int mThreadCount;

	HANDLE *mThreadHandles;

	CRITICAL_SECTION mCSGuardMasterList;  // guards the following resources
	//deque<CToken> mMasterTokenList; // has position and token can appear more than once.
	
	CRITICAL_SECTION mCSGuardTupleLists;  // guards the following resources
	CFreqList mTupleLists[101]; // 0 index is not used.  1 is for 1-tuple, 2 is for 2-tuple, and so on.

	HANDLE mTaskAvailEvent;
	HANDLE mEndEvent;
	HANDLE mTaskCompletedEvent;

	CRITICAL_SECTION mCSFileListIndex; // guards the following resources
	std::deque<CURLPage>* mInputFileList;
	long mCurrFileListIndex;
	long mCurrTargetTupleCount;
	long mCurrFilesProcessed;
	long mRunningDocCount; // cummulative file count across runs, persisted through DB
	long mActualSessionDocCount; // file count for this run, excludes ones that will be ignored.

	P267_FILE *mfpMasterTokens;
	long mMasterTokenCount;

	map<std::string, int>  mStopWords;

	static DWORD WINAPI ThreadLaunch( LPVOID lpParam );

	bool mQueryMode;
protected:
	DWORD ThreadProc();
	bool mergeUSAddress(deque<CToken>& lsTokens, int& wc);
	bool parseFile(CURLPage& thePage);
	bool permuteParagraph(long targetTupleCount, CURLPage& pPage, deque<CToken>& lsTokens, CFreqList& freqTuples);
	bool permuteParagraph_r(long nDepth, long targetTupleCount, CToken& pTokenPrefix, long nStartIdx, long nEndIdx, CURLPage& pPage, deque<CToken>& lsTokens, CFreqList& freqTuples);
	//bool permuteParagraph_r(long nDepth, long targetTupleCount, long nToken1Pos, const char* sPrefix, long nStartIdx, long nEndIdx, CURLPage& pPage, deque<CToken>& lsTokens, CFreqList& freqTuples);
	bool mergeTupleList(long targetTupleCount, CFreqList& freqTuples);
	bool mergeTokenList(deque<CToken>& lsTokens);

public:

	CThreadManager();
	~CThreadManager();

	void InitThreads(int threadcount);
	void EndThreads();

	void QueryMode(bool f);
	bool QueryMode();

	bool parseNtuple(long targetTuple, const string& sFilePrefix);
	bool pruneNtuple(long targetTuple, const string& sFilePrefix);
	void SetInputFileList(std::deque<CURLPage>* fl);
	void toFiles(const string& outfilePrefix);
	void Serialize(long nTupleIndex, const string& outfilePrefix);
	void outputToFile(P267_FILE *ofp, char* filename, deque<CToken>& lsTokens);
	bool isStopWord(const string& theWord, const int pTupleCount);

	void openMasterTokensFile(const string& outfilePrefix);
	void closeMasterTokensFile();
	void pruneDBDocFrequency();

	long MasterTokenCount() { return mMasterTokenCount; }
	void InitFromDB(CDataStorage *pdb);
};

class CSLock
{
public:
    // Acquire the state of the semaphore
    CSLock ( CRITICAL_SECTION & cs )
        : mcs(cs)
    {
		EnterCriticalSection (&mcs);
    }
    // Release the state of the semaphore
    ~CSLock ()
    {
		LeaveCriticalSection (&mcs);
    }
private:
    CRITICAL_SECTION & mcs;
};
