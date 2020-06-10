/*
 * Copyright 2018 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.naps.da;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;
import org.jbpm.process.workitem.core.AbstractLogOrThrowWorkItemHandler;
import org.jbpm.process.workitem.core.util.RequiredParameterValidator;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.jbpm.process.workitem.core.util.Wid;
import org.jbpm.process.workitem.core.util.WidParameter;
import org.jbpm.process.workitem.core.util.WidResult;
import org.jbpm.process.workitem.core.util.service.WidAction;
import org.jbpm.process.workitem.core.util.service.WidAuth;
import org.jbpm.process.workitem.core.util.service.WidService;
import org.jbpm.process.workitem.core.util.WidMavenDepends;

@Wid(widfile="RDBMSWorkItem.wid", name="RDBMSWorkItem",
        displayName="RDBMSWorkItem",
        defaultHandler="mvel: new com.redhat.naps.da.RDBMSWorkItemWorkItemHandler()",
        documentation = "rdbms-workitem/index.html",
        category = "rdbms-workitem",
        icon = "RDBMSWorkItem.png",
        parameters={
            @WidParameter(name="userId", type="new IntegerDataType()", runtimeType="Integer", required = true)
        },
        results={
            @WidResult(name="employeeId")
        },
        mavenDepends={
            @WidMavenDepends(group="com.redhat.naps.da", artifact="rdbms-workitem", version="1.0.1")
        },
        serviceInfo = @WidService(category = "rdbms-workitem", description = "${description}",
                keywords = "",
                action = @WidAction(title = "RDBMSWorkItem"),
                authinfo = @WidAuth(required = true, params = {"dbJndiName", "sqlPrefix"},
                    paramsdescription = {"DataSource JNDI Name", "SQL Statement Prefix"})
        )
)
public class RDBMSWorkItemWorkItemHandler extends AbstractLogOrThrowWorkItemHandler {

    private String dbJndiName;
    private String sqlPrefix = "select patientfullname from patient where id = ";
    private DataSource ds;

    public RDBMSWorkItemWorkItemHandler(String dbJndiNameParam, String sqlPrefixParam) {
        if(dbJndiNameParam == null || dbJndiNameParam.equals(""))
            dbJndiName = System.getProperty("com.redhat.naps.da.dbJndiName");
        else
            this.dbJndiName = dbJndiNameParam;

        if(sqlPrefixParam != null && !sqlPrefixParam.equals(""))
            this.sqlPrefix = sqlPrefixParam;

        System.out.println("RDBMSWorkItemWorkItemHandler():   dbJndiName = "+this.dbJndiName+ " : sqlPrefix = "+this.sqlPrefix);
        InitialContext ic;
        try {
                ic = new InitialContext();
                ds = (DataSource) ic.lookup("java:jboss/" + this.dbJndiName);
        } catch (NamingException e) {
           throw new RuntimeException("Error locating datasource: "+this.dbJndiName, e);
        }
    }

    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        try {
            RequiredParameterValidator.validate(this.getClass(), workItem);

            Integer userId = (Integer) workItem.getParameter("userId");
            System.out.println("executeWorkItem() : userId  = "+userId);

            String fullName = null;
            Connection conn = null;
            try {
                conn = ds.getConnection();
                Statement sObj = conn.createStatement();

                ResultSet rs = sObj.executeQuery(sqlPrefix + userId.intValue());
                while (rs.next()) {
                    fullName = rs.getString(1);
                }

            } catch (SQLException e) {
                throw new RuntimeException("Error while database access", e);
            }finally {
                if( conn != null) {
                    try {
                        conn.close();
                    }catch(Exception x) {
                        x.printStackTrace();
                    }
                }
            }

            // return results
            Map<String, Object> results = new HashMap<String, Object>();
            results.put("employeeId", fullName);


            manager.completeWorkItem(workItem.getId(), results);
        } catch(Throwable cause) {
            handleException(cause);
        }
    }

    @Override
    public void abortWorkItem(WorkItem workItem,
                              WorkItemManager manager) {
    }
}


