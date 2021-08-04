package com.shong.xiong_page.plugin;

import com.shong.xiong_page.bean.Page;
import com.shong.xiong_page.parse.CountSqlParser;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 分页拦截器
 *
 * @auther 10349 XIONGSY
 * @create 2021/8/3
 */
@Intercepts({@Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class})
        , @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class})})
public class PageInterceptor implements Interceptor {

    private static String pageparam_1 = "PAGEPARAM_1";
    private static String pageparam_2 = "PAGEPARAM_2";


    public Object intercept(Invocation invocation) throws Throwable {
        // 检查第一个参数是否是Page.class
        MapperMethod.ParamMap args = (MapperMethod.ParamMap) invocation.getArgs()[1];
        if (ObjectUtils.isEmpty(args) || args.size() <= 0 || !(args.get("param1") instanceof Page)) {
            // 没有page参数，不需要分页
            return invocation.proceed();
        }

        Page<Object> reqPage = (Page<Object>) args.get("param1");
        Page<Object> respPage = new Page<Object>(reqPage.getPageNum(), reqPage.getPageSize());

        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        Object parameter = invocation.getArgs()[1];
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);

        // 先查出数量
        Long count = count(invocation, boundSql);
        respPage.setTotal(count);
        // 组织分页的语句
        List<Object> objects = pageQuery(invocation, boundSql, reqPage);
        respPage.addAll(objects);
        return respPage;
    }


    private <E> List<E> pageQuery(Invocation invocation, BoundSql boundSql, Page page) {
        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        Object parameter = invocation.getArgs()[1];
        RowBounds rowBounds = (RowBounds) invocation.getArgs()[2];
        ResultHandler resultHandler = (ResultHandler) invocation.getArgs()[3];
        Executor executor = (Executor) invocation.getTarget();
        CacheKey cacheKey = executor.createCacheKey(mappedStatement, parameter, rowBounds, boundSql);


        createParamMappingToBoundSql(mappedStatement, boundSql, cacheKey, page);
        createParamObjectToBoundSql(mappedStatement, boundSql, page);
        String pageSql = getPageSql(boundSql);
        BoundSql newboundSql = new BoundSql(mappedStatement.getConfiguration(), pageSql, boundSql.getParameterMappings(), boundSql.getParameterObject());
        List<E> query = null;
        try {
            query = executor.query(mappedStatement, parameter, rowBounds, resultHandler, cacheKey, newboundSql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return query;

    }

    private String getPageSql(BoundSql boundSql) {
        String sql = boundSql.getSql();
        sql += " limit ?,?";
        return sql;
    }

    /**
     * 把分页参数加入boundSql
     *
     * @param mappedStatement
     * @param boundSql
     */
    private void createParamObjectToBoundSql(MappedStatement mappedStatement, BoundSql boundSql, Page page) {
        Object parameterObject = boundSql.getParameterObject();
        MetaObject metaObject = mappedStatement.getConfiguration().newMetaObject(parameterObject);
        metaObject.setValue(pageparam_1, page.getStartRow());
        metaObject.setValue(pageparam_2, page.getPageSize());
    }

    /**
     * 把分页参数映射加入到boundSql
     *
     * @param mappedStatement
     * @param boundSql
     * @param cacheKey
     */
    private void createParamMappingToBoundSql(MappedStatement mappedStatement, BoundSql boundSql, CacheKey cacheKey, Page page) {
        cacheKey.update(page.getStartRow());
        cacheKey.update(page.getPageSize());
        if (boundSql.getParameterMappings() != null) {
            if (boundSql.getParameterMappings().size() == 0) {
                try {
                    Field parameterMappings = boundSql.getClass().getDeclaredField("parameterMappings");
                    parameterMappings.setAccessible(true);
                    parameterMappings.set(boundSql, new ArrayList<ParameterMapping>());
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            boundSql.getParameterMappings().add(new ParameterMapping.Builder(mappedStatement.getConfiguration(), pageparam_1, Integer.class).build());
            boundSql.getParameterMappings().add(new ParameterMapping.Builder(mappedStatement.getConfiguration(), pageparam_2, Integer.class).build());
        }
    }

    /**
     * 查出数据的数量
     *
     * @param invocation
     * @return
     */
    private Long count(Invocation invocation, BoundSql boundSql) {
        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        Object parameter = invocation.getArgs()[1];
        RowBounds rowBounds = (RowBounds) invocation.getArgs()[2];
        ResultHandler resultHandler = (ResultHandler) invocation.getArgs()[3];
        Executor executor = (Executor) invocation.getTarget();

        CacheKey cacheKey = executor.createCacheKey(mappedStatement, parameter, RowBounds.DEFAULT, boundSql);
        CountSqlParser countSqlParser = new CountSqlParser();
        String countSql = countSqlParser.getSmartCountSql(boundSql.getSql());
        // 为count 专门初始化一个boundSql
        BoundSql countBoundSql = new BoundSql(mappedStatement.getConfiguration(), countSql, boundSql.getParameterMappings(), parameter);
        List<Object> query = null;
        try {
            boundSqlAdditionalParameterCopy(countBoundSql, boundSql);
            query = executor.query(createCountMappedStatement(mappedStatement), parameter, rowBounds, resultHandler, cacheKey, countBoundSql);
            return (Long) query.get(0);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return 0L;
    }

    private void boundSqlAdditionalParameterCopy(BoundSql newBoundSql, BoundSql oldBoundSql) throws NoSuchFieldException, IllegalAccessException {
        Field additionalParametersF = oldBoundSql.getClass().getDeclaredField("additionalParameters");
        additionalParametersF.setAccessible(true);
        Map<String, Object> additionalParameters = (Map<String, Object>) additionalParametersF.get(oldBoundSql);
        for (String key : additionalParameters.keySet()) {
            newBoundSql.setAdditionalParameter(key, additionalParameters.get(key));
        }
    }

    private MappedStatement createCountMappedStatement(MappedStatement ms) {
        MappedStatement.Builder statementBuilder = new MappedStatement.Builder(ms.getConfiguration(), ms.getId() + "_COUNT", ms.getSqlSource(), ms.getSqlCommandType())
                .resource(ms.getResource())
                .fetchSize(ms.getFetchSize())
                .timeout(ms.getTimeout())
                .statementType(ms.getStatementType())
                .keyGenerator(ms.getKeyGenerator())
                .databaseId(ms.getDatabaseId())
                .lang(ms.getLang())
                .resultSetType(ms.getResultSetType())
                .flushCacheRequired(ms.isFlushCacheRequired())
                .useCache(ms.isUseCache());
        if (ms.getKeyProperties() != null && ms.getKeyProperties().length != 0) {
            StringBuilder keyProperties = new StringBuilder();
            for (String keyProperty : ms.getKeyProperties()) {
                keyProperties.append(keyProperty).append(",");
            }
            keyProperties.delete(keyProperties.length() - 1, keyProperties.length());
            statementBuilder.keyProperty(keyProperties.toString());
        }
        List<ResultMap> resultMaps = new ArrayList<ResultMap>();
        ResultMap resultMap = new ResultMap.Builder(ms.getConfiguration(), ms.getId(), Long.class, new ArrayList<ResultMapping>()).build();
        resultMaps.add(resultMap);
        statementBuilder.resultMaps(resultMaps);
        MappedStatement statement = statementBuilder.build();
        return statement;
    }
}
