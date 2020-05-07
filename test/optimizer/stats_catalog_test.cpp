#include <utility>

#include "catalog/catalog.h"
#include "catalog/postgres/pg_statistic.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/expression/constant_value_expression.h"
#include "test_util/test_harness.h"
#include "type/transient_value_factory.h"

namespace terrier::optimizer {
struct StatsCatalogTests : public TerrierTest {
  void SetUp() override {
    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();

    auto *txn = txn_manager_->BeginTransaction();
    db_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);

    // Ensure pg_statistic table exists with correct OID
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
    EXPECT_NE(accessor, nullptr);
    VerifyStatsCatalog(*accessor);

    // Initialize pointers to table and index
    statistics_ = accessor->GetTable(catalog::postgres::STATISTIC_TABLE_OID);
    statistics_oid_index_ = accessor->GetIndex(catalog::postgres::STATISTIC_OID_INDEX_OID);

    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  void VerifyStatsCatalog(const catalog::CatalogAccessor &accessor) {
    auto ns_oid = accessor.GetNamespaceOid("pg_catalog");
    EXPECT_EQ(ns_oid, catalog::postgres::NAMESPACE_CATALOG_NAMESPACE_OID);
    auto table_oid = accessor.GetTableOid(ns_oid, "pg_statistic");
    EXPECT_EQ(table_oid, catalog::postgres::STATISTIC_TABLE_OID);
  }

  void GetStatTuplesForTable(common::ManagedPointer<transaction::TransactionContext> txn,
                             const catalog::table_oid_t table_oid, std::vector<storage::TupleSlot> *index_results) {
    const auto &oid_pri = statistics_oid_index_->GetProjectedRowInitializer();
    const auto &oid_prm = statistics_oid_index_->GetKeyOidToOffsetMap();

    const std::unique_ptr<byte[]> buffer_lo(common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize()));
    const std::unique_ptr<byte[]> buffer_hi(common::AllocationUtil::AllocateAligned(oid_pri.ProjectedRowSize()));

    auto *const pr_lo = oid_pri.InitializeRow(buffer_lo.get());
    auto *const pr_hi = oid_pri.InitializeRow(buffer_hi.get());

    // Write the attributes in the ProjectedRow
    // Low key (class, INVALID_COLUMN_OID)
    *(reinterpret_cast<catalog::table_oid_t *>(pr_lo->AccessForceNotNull(oid_prm.at(catalog::indexkeycol_oid_t(1))))) =
        table_oid;
    *(reinterpret_cast<catalog::col_oid_t *>(pr_lo->AccessForceNotNull(oid_prm.at(catalog::indexkeycol_oid_t(2))))) =
        catalog::col_oid_t(0);

    const auto next_oid = catalog::table_oid_t(!table_oid + 1);
    // High key (class + 1, INVALID_COLUMN_OID)
    *(reinterpret_cast<catalog::table_oid_t *>(pr_hi->AccessForceNotNull(oid_prm.at(catalog::indexkeycol_oid_t(1))))) =
        next_oid;
    *(reinterpret_cast<catalog::col_oid_t *>(pr_hi->AccessForceNotNull(oid_prm.at(catalog::indexkeycol_oid_t(2))))) =
        catalog::col_oid_t(0);

    statistics_oid_index_->ScanAscending(*txn, storage::index::ScanType::Closed, 2, pr_lo, pr_hi, 0, index_results);
  }

  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  catalog::db_oid_t db_;
  common::ManagedPointer<storage::SqlTable> statistics_;
  common::ManagedPointer<storage::index::Index> statistics_oid_index_;
};

/*
 * Create and delete a user table.
 */
// NOLINTNEXTLINE
TEST_F(StatsCatalogTests, StatisticTest) {
  // Create a new table in the catalog, backed by a SqlTable
  catalog::table_oid_t table_oid;
  std::set<catalog::col_oid_t> col_oids;
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
    EXPECT_NE(accessor, nullptr);

    // Create the column definition (no OIDs)
    std::vector<catalog::Schema::Column> cols;
    cols.emplace_back("id", type::TypeId::INTEGER, false,
                      parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                      parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    auto tmp_schema = catalog::Schema(cols);

    table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
    EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
    auto schema = accessor->GetSchema(table_oid);

    // Verify we can instantiate a storage object with the generated schema
    auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

    EXPECT_TRUE(accessor->SetTablePointer(table_oid, table));
    EXPECT_EQ(common::ManagedPointer(table), accessor->GetTable(table_oid));

    // Save the column OIDs to check later
    for (const auto &column : schema.GetColumns()) {
      col_oids.insert(column.Oid());
    }
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  // Verify that the corresponding pg_statistic rows exist
  std::set<catalog::col_oid_t> index_col_oids;
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
    EXPECT_NE(accessor, nullptr);

    // Search the statistics index for matching tuples
    std::vector<storage::TupleSlot> index_results;
    GetStatTuplesForTable(common::ManagedPointer(txn), table_oid, &index_results);

    // Initialize ProjectedRowInitializer and ProjectionMap
    const std::vector<catalog::col_oid_t> all_cols{catalog::postgres::PG_STATISTIC_ALL_COL_OIDS.cbegin(),
                                                   catalog::postgres::PG_STATISTIC_ALL_COL_OIDS.cend()};
    storage::ProjectedRowInitializer all_cols_pri = statistics_->InitializerForProjectedRow(all_cols);
    storage::ProjectionMap all_cols_prm = statistics_->ProjectionMapForOids(all_cols);

    // Process all of the statistics tuples returned by the index, by inserting bogus statistics
    const std::unique_ptr<byte[]> buffer(common::AllocationUtil::AllocateAligned(all_cols_pri.ProjectedRowSize()));
    auto *const select_pr = all_cols_pri.InitializeRow(buffer.get());
    for (const auto &slot : index_results) {
      auto UNUSED_ATTRIBUTE result = statistics_->Select(common::ManagedPointer(txn), slot, select_pr);
      EXPECT_TRUE(result);

      const auto *const col_oid = reinterpret_cast<const catalog::col_oid_t *const>(
          select_pr->AccessWithNullCheck(all_cols_prm.at(catalog::postgres::STAATTNUM_COL_OID)));
      EXPECT_TRUE(col_oid != nullptr);

      // Ensure this is one of the columns we created previously
      EXPECT_EQ(col_oids.count(*col_oid), 1);

      // Insert so that we can compare column OIDs later
      index_col_oids.insert(*col_oid);

      // Prepare to update this tuple with some bogus statistics
      auto *const redo = txn->StageWrite(db_, catalog::postgres::STATISTIC_TABLE_OID, all_cols_pri);
      redo->SetTupleSlot(slot);

      // Write the attributes in the Redo Record
      auto relid_entry = reinterpret_cast<catalog::table_oid_t *>(
          redo->Delta()->AccessForceNotNull(all_cols_prm[catalog::postgres::STARELID_COL_OID]));
      auto attnum_entry = reinterpret_cast<catalog::col_oid_t *>(
          redo->Delta()->AccessForceNotNull(all_cols_prm[catalog::postgres::STAATTNUM_COL_OID]));
      auto nullfrac_entry = reinterpret_cast<double *>(
          redo->Delta()->AccessForceNotNull(all_cols_prm[catalog::postgres::STANULLFRAC_COL_OID]));
      auto distinct_entry = reinterpret_cast<double *>(
          redo->Delta()->AccessForceNotNull(all_cols_prm[catalog::postgres::STADISTINCT_COL_OID]));
      auto numrows_entry = reinterpret_cast<uint32_t *>(
          redo->Delta()->AccessForceNotNull(all_cols_prm[catalog::postgres::STA_NUMROWS_COL_OID]));

      *relid_entry = table_oid;
      *attnum_entry = *col_oid;
      *nullfrac_entry = 0.123;
      *distinct_entry = 4.56;
      *numrows_entry = 15721;

      result = statistics_->Update(common::ManagedPointer(txn), redo);
      EXPECT_TRUE(result);

      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }
  }

  // Ensure the sets of column OIDs are equal
  EXPECT_EQ(col_oids, index_col_oids);

  // Ensure that GetTableStats returns the correct statistics
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
    EXPECT_NE(accessor, nullptr);

    auto table_stats = accessor->GetTableStats(table_oid);
    for (catalog::col_oid_t col_oid : col_oids) {
      auto col_stats = table_stats->GetColumnStats(col_oid);

      // TODO(khg): no way to get frac_null_ out of a ColumnStats?
      // EXPECT_EQ(col_stats->GetFracNull(), 0.123);
      EXPECT_EQ(col_stats->GetCardinality(), 4.56);
      EXPECT_EQ(col_stats->GetNumRows(), 15721);
    }

    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  // Check that pg_statistic tuples are deleted when table is dropped
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_);
    EXPECT_NE(accessor, nullptr);

    // Drop table from storage
    accessor->DropTable(table_oid);

    // Search the statistics index for matching tuples
    std::vector<storage::TupleSlot> index_results;
    GetStatTuplesForTable(common::ManagedPointer(txn), table_oid, &index_results);

    // All matching tuples should have been deleted
    EXPECT_EQ(index_results.size(), 0);

    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

}  // namespace terrier::optimizer