
#include "btree.h"

BTreeIndex::BTreeIndex(DbRelation &relation, Identifier name, ColumnNames key_columns, bool unique)
    : DbIndex(relation, name, key_columns, unique), file(this->relation.get_table_name() + '_' + this->name) {
    this->build_key_profile();
    this->stat = nullptr;
    this->root = nullptr;
    this->closed = true;
}

BTreeIndex::~BTreeIndex(){
    delete this->stat;
    delete this->root;
}

void BTreeIndex::create() {
    this->file.create();
    this->stat = new BTreeStat(this->file, STAT, STAT + 1, this->key_profile);
    this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
    this->closed = false;

    Handles* handles = nullptr;
    try{
        handles = this->relation.select();
        for(auto const& handle : *handles)
            insert(handle);
        delete handles;
    } catch(...) {
        delete handles;
        this->file.drop();
        throw;
    }
}

void BTreeIndex::drop(){
    this->file.drop();
    //this->closed = true;
}

void BTreeIndex::open(){
    if(this->closed){
        this->file.open();
        this->stat = new BTreeStat(this->file, STAT, this->key_profile);
        if(this->stat->get_height() == 1)
            this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, false);
        else
            this->root = new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, false);
    }
}

void BTreeIndex::close(){
    this->file.close();
    delete this->stat;
    delete this->root;
    this->stat = nullptr;
    this->root = nullptr;
    this->closed = true;
}

Handles* BTreeIndex::lookup(ValueDict *key) const{
    KeyValue* kval = tkey(key);
    Handles* handles = _lookup(this->root, this->stat->get_height(), kval);
    delete kval;
    return handles;
}

Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue *key) const{
    //at leaf
    if(height == 1){
        BTreeLeaf* leaf = (BTreeLeaf*)node;
        Handles* handles = new Handles();
        try{
            Handle handle = leaf->find_eq(key);
            handles->push_back(handle);
        }catch (std::out_of_range& exc){ ; }

        return handles;
    }//not at leaf
    else{
        BTreeInterior* interior = (BTreeInterior*)node;
        return _lookup(interior->find(key, height), height-1, key);
    }
}

Handles* BTreeIndex::range(ValueDict *min_key, ValueDict *max_key) const{
    throw DbRelationError("range is not implemented");
}

void BTreeIndex::insert(Handle handle){
    ValueDict *row = this->relation.project(handle, &this->key_columns);
    KeyValue* kval = tkey(row);
    delete row;

    Insertion split_root = this->_insert(this->root, this->stat->get_height(), kval, handle);

    if(!BTreeNode::insertion_is_none(split_root)){
        BlockID rroot = split_root.first;
        KeyValue boundary = split_root.second;
        BTreeInterior* root = new BTreeInterior(this->file, 0, this->key_profile, true);
        root->insert(&boundary, rroot);
        this->stat->set_root_id(root->get_id());
        u_int height = this->stat->get_height();
        height++;
        this->stat->set_height(height);
        this->stat->save();
        this->root = root;
    }

    delete kval;
}

Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue *key, Handle handle){
    if(height == 1){
        BTreeLeaf* leaf = (BTreeLeaf*)node;
        return leaf->BTreeLeaf::insert(key, handle);
    }
    else{
        BTreeInterior* interior = (BTreeInterior*)node;
        Insertion new_kid = this->_insert(interior->find(key, height), height - 1, key, handle);
        if(!BTreeNode::insertion_is_none(new_kid)){
            BlockID nnode = new_kid.first;
            KeyValue boundary = new_kid.second;
            return interior->BTreeInterior::insert(&boundary, nnode);
        }
        return BTreeNode::insertion_none();
    }
}

void BTreeIndex::del(Handle handle){
    throw DbRelationError("delete is not supported yet");
}

KeyValue* BTreeIndex::tkey(const ValueDict *key) const{
    KeyValue* kval = new KeyValue();
    for(auto const& columns : this->key_columns){
        kval->push_back(key->at(columns));
    }
    return kval;
}

void BTreeIndex::build_key_profile(){
    ColumnAttributes* columnAttributes;
    columnAttributes = this->relation.get_column_attributes(this->key_columns);

    for(auto ca : *columnAttributes){
        this->key_profile.push_back(ca.get_data_type());
    }
}


void test_set_row_btree(ValueDict &row, int a, std::string b) {
    row["a"] = Value(a);
    row["b"] = Value(b);
}

bool test_btree(){
    ColumnNames columnNames;
    columnNames.push_back("a");
    columnNames.push_back("b");
    ColumnAttributes columnAttributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    columnAttributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    columnAttributes.push_back(ca);
    HeapTable table1("foo", columnNames, columnAttributes);
    table1.create();
    std::cout << "table create good" << std::endl;

    ValueDict rowItr;
    std::string b = "alkjsl;kj; as;lkj;alskjf;laks df;alsdkjfa;lsdkfj ;alsdfkjads;lfkj a;sldfkj a;sdlfjk a";
    Handle tableInsertHandle;

    for(int i = 0; i < 1000; i++) {
        if(i != 510) {
            test_set_row_btree(rowItr, i, b);
            tableInsertHandle = table1.insert(&rowItr);
        }
    }

    BTreeIndex testIndex(table1, "fooIndex", columnNames, false);
    testIndex.create();
    std::cout << "create good" << std::endl;
    /*
    testIndex.close();
    std::cout << "close good" << std::endl;
    testIndex.open();
    std::cout << "open good" << std::endl;
    */
    ValueDict afterCreationInsert;
    test_set_row_btree(afterCreationInsert, 510, b);
    Handle inserted = table1.insert(&afterCreationInsert);
    testIndex.insert(inserted);
    std::cout << "insert good" << std::endl;

    ValueDict lookupRes;
    Handles lookupHandles = *testIndex.lookup(&afterCreationInsert);
    for(auto const& handle : lookupHandles){
        lookupRes = *table1.project(handle);
        if(lookupRes["a"] != 510){
            std::cout << "lookup failed: false" << std::endl;
            return false;
        }
    }
    std::cout << "lookup good" << std::endl;

    testIndex.drop();
    std::cout << "drop good" << std::endl;
    table1.drop();
    return true;
}