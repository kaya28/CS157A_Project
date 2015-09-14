#ifndef _URLPAGE_H
#define _URLPAGE_H

#pragma once
class CURLPage
{
	long mID;
	string mURL; // the real URL
	string mFilename; // filename stored in local drive
	long mWordCount;
	bool mIgnore;
public:
	CURLPage();
	CURLPage(long id, const string& pFilename, const string& pURL);
	~CURLPage(void);

	void Clear();
	void Set(long id, const string& pFilename, const string& pURL);

	bool Ignore() { return mIgnore;}
	void Ignore(bool b) { mIgnore=b; }

	const string& URL() { return mURL; }
	void URL(const string& s) { mURL=s; }

	const string& Filename() { return mFilename; }
	void Filename(const string& s) { mFilename=s; }

	long ID() { return mID; }
	void ID(long id) { mID=id; }

	long WordCount() { return mWordCount; }
	void WordCount(long i) { mWordCount=i; }
};

#endif 
