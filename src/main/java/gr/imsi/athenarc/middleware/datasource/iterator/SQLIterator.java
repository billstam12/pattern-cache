package gr.imsi.athenarc.middleware.datasource.iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class SQLIterator<T> implements Iterator<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(SQLIterator.class);

    protected final ResultSet resultSet;
    protected boolean hasNextCached = false;
    protected boolean nextExists = false;

    protected SQLIterator(ResultSet resultSet) {
        if (resultSet == null) {
            throw new IllegalArgumentException("ResultSet cannot be null");
        }
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        if (!hasNextCached) {
            try {
                nextExists = resultSet.next();
                hasNextCached = true;
            } catch (SQLException e) {
                LOG.error("Error checking for next result", e);
                nextExists = false;
            }
        }
        return nextExists;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements to iterate over");
        }
        
        hasNextCached = false;
        return getNext();
    }

    protected long getTimestampFromResultSet(String columnName) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnName);
        return timestamp != null ? timestamp.getTime() : 0L;
    }

    protected Double getSafeDoubleValue(String columnName) throws SQLException {
        double value = resultSet.getDouble(columnName);
        return resultSet.wasNull() ? null : value;
    }

    protected String getSafeStringValue(String columnName) throws SQLException {
        return resultSet.getString(columnName);
    }

    protected abstract T getNext();
}
