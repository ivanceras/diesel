use super::{Statement, Binds, ffi, libc};
use result::QueryResult;
use row::Row;
use mysql::Mysql;

/// Users of this type must completely finish with the result of each call to
/// `.next` before the next call to `.next`, as the same underlying buffer is
/// being reused each call.
pub struct StatementIterator<'a> {
    stmt: &'a mut Statement,
    output_binds: Binds,
}

impl<'a> StatementIterator<'a> {
    pub fn new(stmt: &'a mut Statement) -> QueryResult<Self> {
        use result::Error::QueryBuilderError;

        let mut result_metadata = match try!(stmt.result_metadata()) {
            Some(result) => result,
            None => return Err(QueryBuilderError("Attempted to get results \
                on a query with no results".into())),
        };
        let result_types = result_metadata.fields().map(|f| f.type_);
        let mut output_binds = Binds::from_output_types(result_types);

        unsafe {
            ffi::mysql_stmt_bind_result(
                stmt.stmt,
                output_binds.mysql_binds().as_mut_ptr(),
            );
        }
        stmt.did_an_error_occur()?;

        Ok(StatementIterator {
            stmt: stmt,
            output_binds: output_binds,
        })
    }
}

impl<'a> Iterator for StatementIterator<'a> {
    type Item = QueryResult<MysqlRow>;

    fn next(&mut self) -> Option<Self::Item> {
        use std::mem::transmute;

        self.output_binds.reset_dynamic_buffers();
        let next_row_result = unsafe { ffi::mysql_stmt_fetch(self.stmt.stmt) };
        match next_row_result as libc::c_uint {
            ffi::MYSQL_NO_DATA => return None,
            ffi::MYSQL_DATA_TRUNCATED => self.output_binds.populate_dynamic_buffers(self.stmt.stmt),
            _ => {} // Either success or error which we check on the next line
        }
        match self.stmt.did_an_error_occur() {
            Err(e) => return Some(Err(e)),
            Ok(_) => {} // continue
        }

        // This is a bit tricky since we're actually mutating the same underlying
        // buffer, and yielding it over and over again. stdlib doesn't have an
        // iterator abstraction for "and you must finish with the item before
        // the next call to next" though. This would be horrendously unsafe
        // if we were to make this type public.
        let this_is_totally_static_i_promise = unsafe { transmute(&self.output_binds) };

        Some(Ok(MysqlRow {
            col_idx: 0,
            binds: this_is_totally_static_i_promise,
        }))
    }
}

pub struct MysqlRow {
    col_idx: usize,
    binds: &'static Binds,
}

impl Row<Mysql> for MysqlRow {
    fn take(&mut self) -> Option<&[u8]> {
        let current_idx = self.col_idx;
        self.col_idx += 1;
        self.binds.field_data(current_idx)
    }

    fn next_is_null(&self, count: usize) -> bool {
        (0..count).all(|i| self.binds.field_data(self.col_idx + i).is_none())
    }
}
