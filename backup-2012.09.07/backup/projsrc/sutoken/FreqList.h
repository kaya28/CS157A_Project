#ifndef _FREQLIST_H
#define _FREQLIST_H

class CDataStorage;
class CURLPage;

class CFreqList
{
	map<string, CToken> mList;
	bool mQueryMode;

	void DBSerialize(P267_FILE *ofp, CDataStorage *pDB, long nTupleIndex, long totalTokenCount, long totalDocCount);
	void QueryMatchup(P267_FILE *ofp, CDataStorage *pDB, long nTupleIndex, long totalTokenCount, long totalDocCount);

public:
	CFreqList() { mQueryMode = false; }
	~CFreqList() {}

	void QueryMode(bool f) { mQueryMode=f; }
	bool QueryMode() { return mQueryMode; }

	void clear() { mList.clear(); }

	bool exists(const string& tokenKey);
	long DocFreq(const string& tokenKey);

	//used during retrieve from DB, does not assign new DBID
	void InitAdd(CToken& token);  

	// Increment token count by 1, add if not exist.
	void LogToken(CToken& pToken, CURLPage& pPage);

	// Take input token count and add it into total count.
	void mergeToken(CToken& token, long& nextDBID);

	long size() { return (long) mList.size(); }
	map<string, CToken>& getList() { return mList; }

	void Serialize(P267_FILE *ofp, CDataStorage *pDB, long nTupleIndex, long totalTokenCount, long totalDocCount);

	// percent are 0.0 to 1.0
	bool pruneDocFreq(P267_FILE *ofp, double bottomPercent, double topPercent, long totalDocCount);

	// prune term frequency within a file
	bool pruneTermFreq(double bottomPercent, double topPercent);
};

#endif
