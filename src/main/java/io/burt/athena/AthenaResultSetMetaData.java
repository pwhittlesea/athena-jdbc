package io.burt.athena;

import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.ResultSetMetadata;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

public class AthenaResultSetMetaData implements ResultSetMetaData {
    private final QueryExecution queryExecution;
    private final ResultSetMetadata metaData;

    public AthenaResultSetMetaData(QueryExecution queryExecution, ResultSetMetadata metaData) {
        this.queryExecution = queryExecution;
        this.metaData = metaData;
    }

    private ColumnInfo getColumn(int n) {
        return metaData.columnInfo().get(n - 1);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return metaData.columnInfo().size();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).label();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).name();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return getColumn(column).tableName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return getColumn(column).schemaName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getColumn(column).catalogName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).precision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).scale();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        switch (getColumn(column).type()) {
            case "tinyint":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "integer":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "decimal":
                return Types.DECIMAL;
            case "boolean":
                return Types.BOOLEAN;
            case "char":
                return Types.CHAR;
            case "varchar":
            case "json":
            case "interval day to second":
            case "interval year to month":
                return Types.VARCHAR;
            case "varbinary":
                return Types.VARBINARY;
            case "date":
                return Types.DATE;
            case "time":
                return Types.TIME;
            case "time with time zone":
                return Types.TIME_WITH_TIMEZONE;
            case "timestamp":
                return Types.TIMESTAMP;
            case "timestamp with time zone":
                return Types.TIMESTAMP_WITH_TIMEZONE;
            case "array":
                return Types.ARRAY;
            case "map":
            case "row":
                return Types.STRUCT;
            default:
                return Types.OTHER;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).type();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        String typeName = getColumn(column).type();
        if (typeName.equals("varchar")) {
            return String.class.getName();
        }
        return Object.class.getName();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calculating column display sizes is not supported");
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumn(column).caseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        getColumn(column);
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        switch (getColumn(column).nullable()) {
            case NULLABLE: return ResultSetMetaData.columnNullable;
            case NOT_NULL: return ResultSetMetaData.columnNoNulls;
            case UNKNOWN: return ResultSetMetaData.columnNullableUnknown;
            default: return ResultSetMetaData.columnNullableUnknown;
        }
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        String typeName = getColumn(column).type();
        return typeName.equals("integer")
                || typeName.equals("bigint")
                || typeName.equals("smallint")
                || typeName.equals("tinyint")
                || typeName.equals("float")
                || typeName.equals("double")
                || typeName.equals("decimal");
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        getColumn(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        getColumn(column);
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format("%s is not a wrapper for %s", this.getClass().getName(), iface.getName()));
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    public String getQueryExecutionId() {
        return queryExecution.queryExecutionId();
    }

    public String getOutputLocation() {
        return queryExecution.resultConfiguration().outputLocation();
    }
}
