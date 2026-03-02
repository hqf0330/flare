package com.bhcode.flare.flink.conf;

import com.bhcode.flare.common.util.PropUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlareFlinkConfTest {

    private String newKeyBackup;
    private String runtimeScheduleEnableBackup;
    private String runtimeSchedulePoolSizeBackup;

    @Before
    public void setUp() {
        this.newKeyBackup = PropUtils.getString(FlareFlinkConf.FLINK_JOB_AUTO_START);
        this.runtimeScheduleEnableBackup = PropUtils.getString(FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_ENABLE);
        this.runtimeSchedulePoolSizeBackup = PropUtils.getString(FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_POOL_SIZE);
    }

    @After
    public void tearDown() {
        PropUtils.setProperty(
                FlareFlinkConf.FLINK_JOB_AUTO_START,
                this.newKeyBackup == null ? "" : this.newKeyBackup
        );
        PropUtils.setProperty(
                FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_ENABLE,
                this.runtimeScheduleEnableBackup == null ? "" : this.runtimeScheduleEnableBackup
        );
        PropUtils.setProperty(
                FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_POOL_SIZE,
                this.runtimeSchedulePoolSizeBackup == null ? "" : this.runtimeSchedulePoolSizeBackup
        );
    }

    @Test
    public void shouldReadNormalizedKey() {
        PropUtils.setProperty(FlareFlinkConf.FLINK_JOB_AUTO_START, "false");
        Assert.assertFalse(FlareFlinkConf.isJobAutoStart());
    }

    @Test
    public void shouldUseDefaultWhenMissing() {
        PropUtils.setProperty(FlareFlinkConf.FLINK_JOB_AUTO_START, "");
        Assert.assertTrue(FlareFlinkConf.isJobAutoStart());
    }

    @Test
    public void shouldReadRuntimeScheduleEnable() {
        PropUtils.setProperty(FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_ENABLE, "false");
        Assert.assertFalse(FlareFlinkConf.isRuntimeScheduleEnable());
    }

    @Test
    public void shouldClampInvalidRuntimeSchedulePoolSize() {
        PropUtils.setProperty(FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_POOL_SIZE, "0");
        Assert.assertEquals(1, FlareFlinkConf.getRuntimeSchedulePoolSize());
    }
}
