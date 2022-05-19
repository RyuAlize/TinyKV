#include<atomic>
#include <iostream>
#include<string>
#include<thread>
#include<unordered_map>
#include"kvs.h"


int compare_info(const char* array1, const char* array2)
{
    int i;
    int response = 0;
    i = 0;

    while (array1[i] == array2[i] && response == 0)
    {
        if (array1[i] == '\0' || array2[i] == '\0') {
            response = 1;
        }
        i++;
    }

    return response;
}

class Person : public KVType {
public:
    Person(){}
    Person(std::string name, int age, std::string sex):name(name),age(age),sex(sex){}
    ~Person(){}

    void serlize(Writer<StringBuffer>* writer) {
        
        writer->String("name");
        writer->String(name.c_str());
        writer->String("age");
        writer->Int(age);
        writer->String("sex");
        writer->String(sex.c_str());

    }
    KVType* deserlize(Value& doc) {
        for (Value::ConstMemberIterator m = doc.MemberBegin(); m != doc.MemberEnd(); ++m) {
            std::cout << m->name.GetString() << std::endl;
            if (compare_info(m->name.GetString(),"name")) {
                this->name = m->value.GetString();
            }
            else if (compare_info(m->name.GetString(), "age")) {
                this->age = m->value.GetInt();
            }
            else if (compare_info(m->name.GetString(), "sex")) {
                this->sex = m->value.GetString();
            }
            
        }
        return this;
    }

private:
    std::string name;
    int age;
    std::string sex;
};

class Key : public KVType {
public:
    Key(){}
    Key(int id):id(id){}
    ~Key(){}

    virtual void serlize(Writer<StringBuffer>* writer) {      
        writer->StartObject();
        writer->String("id");
        writer->Int(id);
        writer->EndObject();
    }

    bool operator < (const Key& other) {
        return this->id < other.id;
    }
    bool operator == (const Key& other) {
        return this->id == other.id;
    }
    bool operator != (const Key& other) {
        return this->id != other.id;
    }
    KVType* deserlize(Value& doc) {
        for (Value::ConstMemberIterator m = doc.MemberBegin(); m != doc.MemberEnd(); ++m) {
            std::cout << m->name.GetString() << std::endl;
            auto t = m->name.GetString();
            if (compare_info(m->name.GetString(), "id")) {
                this->id = m->value.GetInt();
            }
        }
        return this;
    }

private:
    int id;
};

class Val :public KVType {
public:
    Val() {}
    Val(int val) :val(val) {}
    ~Val() {}

    virtual void serlize(Writer<StringBuffer>* writer) {
        writer->StartObject();
        writer->String("val");
        writer->Int(val);
        writer->EndObject();
    }

    KVType* deserlize(Value& doc) {
        for (Value::ConstMemberIterator m = doc.MemberBegin(); m != doc.MemberEnd(); ++m) {
            std::cout << m->name.GetString() << std::endl;
            auto t = m->name.GetString();
            if (compare_info(m->name.GetString(), "val")) {
                this->val= m->value.GetInt();
            }
        }
        return this;
    }
    friend std::ostream& operator << (std::ostream& os, Val& v) {
        os << "val:" << v.val;
        return os;
    }
private:

    int val;
};


int main()
{
    //StringBuffer sb;
    //PrettyWriter<StringBuffer> writer(sb);
    //Key k(1);
    //Val v(1);
    //CommandSet<Key, Val> cmdset(&k,&v);
    //const char* p = cmdset.serlizeCommand();

    
    KVStore<Key, Val> kvs("D:\\work\\db");


    //for (int i = 0; i < 10; i++) {
    //    Key id(i);
    //    Val val(i);
    //    kvs.set(&id, &val);
    //}
    //Key id1(1);
    //kvs.remove(&id1);
    //
    //Key id2(3);
    //kvs.remove(&id2);

    //Key id3(5);
    //kvs.remove(&id3);


    for (int i = 0; i < 10; i++) {
        Key id(i);
        Val* val = kvs.get(&id);
        if (val != nullptr)
            std::cout << *val << std::endl;
    }


}

