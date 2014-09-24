package com.saggezza.lubeinsights.platform.core.common.metadata;

import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlow;
import org.apache.log4j.Logger;

/**
 * Created by chiyao on 8/7/14.
 */
public class MetadataRepository {

    public static final Logger logger = Logger.getLogger(MetadataRepository.class);
    public static final ServiceConfig config = ServiceConfig.load();


    /**
     * set metadata value for name without validation
     * @param mdType
     * @param name
     * @param value
     */
    public static final void set(MetadataType mdType, String name, String value) {
        set(mdType, name, value, null, false); // no validation by default
    }

    /**
     * set metadata value for name optional validation
     * @param mdType
     * @param name
     * @param value
     * @param validate
     */
    public static final void set(MetadataType mdType, String name, String value, boolean validate) {
        set(mdType, name, value, null, validate); // no validation by default
    }


    /**
     * This is not a public API, but an internal utility method
     * set metadata value for name and version with optional validation
     * @param mdType
     * @param name
     * @param value
     * @param version
     * @param validate
     */
    private static final void set(MetadataType mdType, String name, String value, String version, boolean validate) {
        try {
            if (validate) {  // make sure value is valid. Otherwise, log error
                try {
                    switch (mdType) {
                        case DATA_MODEL:
                            DataModel.fromJson(value);
                            break;
                        case WORKFLOW:
                            WorkFlow.fromJson(value);
                            break;
                        case SERVICE_COMMAND:
                        case SCHEDULE:
                            break;
                    }
                } catch (Exception e) {
                    logger.trace("Invalid metadata for "+ mdType + "/"+ name + ": " + value );
                    return;
                }
            }
            ZKUtil.zkSet(zkPath(mdType.name(), name, version), value);
        } catch (Exception e) {
            logger.trace("Failed to set metadata for "+ mdType.name() + ":" + name, e);
        }
    }

    /**
     * get the unversioned metadata object for mdtype and name
     * @param mdType
     * @param name
     * @return
     */
    public static final String get(MetadataType mdType, String name) {
        try {
            return ZKUtil.zkGet(zkPath(mdType.name(), name));
        } catch (Exception e) {
            logger.trace("Failed to get metadata for "+ mdType.name() + ":" + name, e);
            e.printStackTrace();
            return null;
        }
    }


    /**
     * stamp a saved metadata object with a version in MetaDataRepository
     * (caller is responsible for version number management)
     * @param mdType
     * @param name
     * @param version
     */
    public static final void stampVersion(MetadataType mdType, String name, String version) throws MetadataException {
        // get the unversioned value first
        String value = get(mdType, name);
        if (value==null) {
            throw new MetadataException("Metadata does not exist: "+ mdType + "/" +name);
        }
        // save it to version
        set(mdType,name,version,true);
    }


    /**
     * define the node path based on mdType, name, and version
     * @param mdType
     * @param name
     * @return
     */
    public static final String zkPath(String mdType, String name, String version) {
        StringBuilder sb =  new StringBuilder("/").append(getTenant()).append("/metadata/")
                .append(getApplication()).append("/")
                .append(mdType).append("/")
                .append(name);
        if (version != null) {
            sb.append("/").append(version);
        }
        return sb.toString();
    }

    /**
     * The unversioned metadata is saves under the tag "open"
     * @param mdType
     * @param name
     * @return
     */
    public static final String zkPath(String mdType, String name) {
        return zkPath(mdType,name,"open");
    }

    public static final String getTenant() {
        String tenant = config.get("tenant");
        return (tenant==null ? "tenant0" : tenant);
    }

    public static final String getApplication() {
        String application = config.get("application");
        return (application == null ? "application0" : application);
    }


    public static final void main(String[] args) {
        String dmJson = "{\"feild1\":\"int\",\"field2\":\"string\"}";
        set(MetadataType.DATA_MODEL,"dm1",dmJson);
        String value = get(MetadataType.DATA_MODEL,"dm1");
        System.out.println(value);
        ZKUtil.close();
    }

}
