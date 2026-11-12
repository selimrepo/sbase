package com.sbase.platform.bi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.javamail.JavaMailSender;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.Executor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class GovernedBiScheduleWorkerTenantIsolationTest {
    @Test
    void scheduleLeaseAndNextRunUpdatesAreTenantScoped() throws Exception {
        JdbcTemplate db=mock(JdbcTemplate.class);
        BiWorkspaceService bi=mock(BiWorkspaceService.class);
        @SuppressWarnings("unchecked") ObjectProvider<JavaMailSender> mail=mock(ObjectProvider.class);
        Executor executor=Runnable::run;
        GovernedBiScheduleWorker worker=new GovernedBiScheduleWorker(db,bi,new ObjectMapper(),executor,mail,"./build/test-bi");
        Class<?> scheduleType=Class.forName("com.sbase.platform.bi.GovernedBiScheduleWorker$Schedule");
        Constructor<?> constructor=scheduleType.getDeclaredConstructor(UUID.class,UUID.class,UUID.class,
            String.class,String.class,String.class,String.class,String.class);
        constructor.setAccessible(true);
        UUID scheduleId=UUID.randomUUID(),tenant=UUID.randomUUID(),report=UUID.randomUUID();
        Object schedule=constructor.newInstance(scheduleId,tenant,report,"alice","0 0 * * * *","UTC","JSON",null);
        Method advance=GovernedBiScheduleWorker.class.getDeclaredMethod("advance",scheduleType,boolean.class,String.class);
        advance.setAccessible(true);
        advance.invoke(worker,schedule,true,null);
        org.mockito.ArgumentCaptor<String> sql=org.mockito.ArgumentCaptor.forClass(String.class);
        verify(db).update(sql.capture(),any(Object[].class));
        assertTrue(sql.getValue().contains("WHERE id=? AND tenant_id=? AND lease_owner=?"));
    }
}
