#include<vector>
#include<algorithm>
#include<unordered_map>
#include<functional>
#include<mutex>
#include<unordered_map>

template<typename LruKey, typename LruValue, typename ClockHandInteger = size_t>
class LruClockCache
{
public:
	LruClockCache(ClockHandInteger numElements,
		const std::function<LruValue(LruKey)>& readMiss,
		const std::function<void(LruKey, LruValue)>&
		writeMiss) :size(numElements), loadData(readMiss), saveData(writeMiss)
	{
		ctr = 0;
		ctrEvict = numElements / 2;

		for (ClockHandInteger i = 0; i < numElements; i++)
		{
			valueBuffer.push_back(LruValue());
			chanceToSurviveBuffer.push_back(0);
			isEditedBuffer.push_back(0);
			keyBuffer.push_back(LruKey());
		}
		mapping.reserve(numElements);
	}

	inline
		const LruValue get(const LruKey& key)  noexcept
	{
		return accessClock2Hand(key, nullptr);
	}

	inline
		const std::vector<LruValue> getMultiple(const std::vector<LruKey>& key)  noexcept
	{
		const int n = key.size();
		std::vector<LruValue> result(n);

		for (int i = 0; i < n; i++)
		{
			result[i] = accessClock2Hand(key[i], nullptr);
		}
		return result;
	}

	inline
		const LruValue getThreadSafe(const LruKey& key)  noexcept
	{
		std::lock_guard<std::mutex> lg(mut);
		return accessClock2Hand(key, nullptr);
	}

	inline
		void set(const LruKey& key, const LruValue& val) noexcept
	{
		accessClock2Hand(key, &val, 1);
	}

	inline
		void setThreadSafe(const LruKey& key, const LruValue& val)  noexcept
	{
		std::lock_guard<std::mutex> lg(mut);
		accessClock2Hand(key, &val, 1);
	}

	void flush()
	{
		for (auto mp = mapping.cbegin();
			mp != mapping.cend(); ++mp)
		{
			if (isEditedBuffer[mp->second] == 1)
			{
				isEditedBuffer[mp->second] = 0;
				auto oldKey = keyBuffer[mp->second];
				auto oldValue = valueBuffer[mp->second];
				saveData(oldKey, oldValue);
				mp = mapping.erase(mp);    
			}
			else
			{
				++mp;
			}
		}
	}

	LruValue const accessClock2Hand(const LruKey& key, const LruValue* value,
		const bool opType = 0)
	{
		typename std::unordered_map<LruKey, ClockHandInteger>::iterator it = mapping.find(key);
		if (it != mapping.end())
		{
			chanceToSurviveBuffer[it->second] = 1;
			if (opType == 1)
			{
				isEditedBuffer[it->second] = 1;
				valueBuffer[it->second] = *value;
			}
			return valueBuffer[it->second];
		}
		else 
		{
			long long ctrFound = -1;
			LruValue oldValue;
			LruKey oldKey;
			while (ctrFound == -1)
			{				
				if (chanceToSurviveBuffer[ctr] > 0)
				{
					chanceToSurviveBuffer[ctr] = 0;
				}

				ctr++;
				if (ctr >= size)
				{
					ctr = 0;
				}

				if (chanceToSurviveBuffer[ctrEvict] == 0)
				{
					ctrFound = ctrEvict;
					oldValue = valueBuffer[ctrFound];
					oldKey = keyBuffer[ctrFound];
				}

				ctrEvict++;
				if (ctrEvict >= size)
				{
					ctrEvict = 0;
				}
			}

			if (isEditedBuffer[ctrFound] == 1)
			{
				if (opType == 0)
				{
					isEditedBuffer[ctrFound] = 0;
				}

				saveData(oldKey, oldValue);

				if (opType == 0)
				{
					const LruValue&& loadedData = loadData(key);
					mapping.erase(keyBuffer[ctrFound]);
					valueBuffer[ctrFound] = loadedData;
					chanceToSurviveBuffer[ctrFound] = 0;

					mapping.emplace(key, ctrFound);
					keyBuffer[ctrFound] = key;

					return loadedData;
				}
				else 
				{
					mapping.erase(keyBuffer[ctrFound]);

					valueBuffer[ctrFound] = *value;
					chanceToSurviveBuffer[ctrFound] = 0;

					mapping.emplace(key, ctrFound);
					keyBuffer[ctrFound] = key;
					return *value;
				}
			}
			else 
			{
				if (opType == 1)
				{
					isEditedBuffer[ctrFound] = 1;
				}

				if (opType == 0)
				{
					const LruValue&& loadedData = loadData(key);
					mapping.erase(keyBuffer[ctrFound]);
					valueBuffer[ctrFound] = loadedData;
					chanceToSurviveBuffer[ctrFound] = 0;

					mapping.emplace(key, ctrFound);
					keyBuffer[ctrFound] = key;

					return loadedData;
				}
				else 
				{
					mapping.erase(keyBuffer[ctrFound]);

					valueBuffer[ctrFound] = *value;
					chanceToSurviveBuffer[ctrFound] = 0;

					mapping.emplace(key, ctrFound);
					keyBuffer[ctrFound] = key;
					return *value;
				}
			}
		}
	}

private:
	ClockHandInteger size;
	std::mutex mut;
	std::unordered_map<LruKey, ClockHandInteger> mapping;
	std::vector<LruValue> valueBuffer;
	std::vector<unsigned char> chanceToSurviveBuffer;
	std::vector<unsigned char> isEditedBuffer;
	std::vector<LruKey> keyBuffer;
	const std::function<LruValue(LruKey)>  loadData;
	const std::function<void(LruKey, LruValue)>  saveData;
	ClockHandInteger ctr;
	ClockHandInteger ctrEvict;
};