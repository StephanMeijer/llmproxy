use super::database::Database;

impl Database {
    /// Closes the pools; `Drop` then drops the database.
    pub async fn close(self) {
        self.pool.close().await;
        self.admin.close().await;
    }
}
