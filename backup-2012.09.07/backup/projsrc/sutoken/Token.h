#ifndef _TOKEN_H
#define _TOKEN_H

#include "stdafx.h"
#include "TokenizerUtils.h"

extern bool SHOWPRUNE;

class CToken
{
	long mDBID;
	bool mDirty; // true if need to save to DB

	string mToken;
	string mTokenOrig; // original unaltered string
	long mFirstPos; // first word position
	long mDistance; // difference from first word pos to last pos
	long mFreq; // count of occurrences
	long mDocFreq; //count of documents it exists in
	int mPerm; // 1 indicates permanent, 0 temporary/discovered
	set<long> mURLList; // list of URL where it was found

	string mLastDocFound; //doc name where it was found last

	struct stemmer* mStemmer;
	char mStemmerBuff[101];
public:
	CToken::CToken();
	CToken::CToken(const CToken& c);
	CToken::~CToken();

	const char* getToken() { return mToken.c_str(); }
	const char* getTokenOrig() { return mTokenOrig.c_str(); }

	const string& Token() { return mToken; }
	string Key() { return mToken; }
	
	const long FirstPos() { return mFirstPos; }
	void FirstPos(long p) { mFirstPos=p; Dirty(true); }

	const long Distance() { return mDistance; }
	void Distance(long p) { mDistance=p; Dirty(true); }

	const long Freq() { return mFreq; }
	const long DocFreq() { return mDocFreq; }

	void Freq(long freq) { mFreq = freq; }
	void DocFreq(long freq) { mDocFreq = freq; Dirty(true); }
	bool StemIt();
	
	long DBID() 
	{ 
		return mDBID; 
	}
	void DBID(long dbid) 
	{ 
		mDBID=dbid; 
		Dirty(true); 
	}

	int Perm() { return mPerm; }
	void Perm(int p) { mPerm=p; Dirty(true); }

	void LastDocFound(const string &filename) { mLastDocFound=filename; Dirty(true); }
	const string& LastDocFound() { return mLastDocFound; }

	void clear();
	void set(const char* t, long pos) ;	
	void set(const char* t, const char* tOrig, long pos) ;	
	void IncrFreq();
	void IncrFreq(const string& docName);

	void AddURLID(long pID);
	std::set<long>& GetURLList();

	bool Dirty() { return mDirty; }
	void Dirty(bool b) { mDirty=b; }
};

#endif
