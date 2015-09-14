

class CConfig
{
	bool mQueryMode;
	string mSQLDSN;
	string mOutfilePrefix;
	long mNTupleCount;
	//deque<string> mInputFilelist;
	deque<CURLPage> mInputFilelist;
	long mThreadCount;
public:
	CConfig(void);
	~CConfig(void);
	bool parseCommandLine(int argc, char** argv);

	long ThreadCount() { return mThreadCount; }
	long NTupleCount() { return mNTupleCount; }

	void QueryMode(bool f) { mQueryMode=f; }
	bool QueryMode() { return mQueryMode; }

	//deque<string>& InputFileList() { return mInputFilelist; }
	deque<CURLPage>& InputFileList() { return mInputFilelist; }

	const string& OutfilePrefix() { return mOutfilePrefix; }
	bool parseConfig(void);

	const string& SQLDSN() { return mSQLDSN; }
	void SQLDSN(const string& dsn) { mSQLDSN=dsn; }
};

