package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import java.util.ArrayList;

/**
 * Created by chiyao on 9/27/14.
 */
public class FieldAddress {

    protected Object[] coordinate;

    /**
     * translate "f1.f2.[3]" to Object[] {"f1","f2", 3}
     * @param address
     * @return
     */
    public FieldAddress(String address) {
        if (address == null) {
            this.coordinate =  null;
        }
        String[] parts = address.split("\\.");
        ArrayList coordinate = new ArrayList();
        for (String part : parts) {
            if (part.endsWith("]")) { // it is list access like A[n]
                int bracketPos = part.indexOf('[');
                if (bracketPos > 0) {
                    coordinate.add(part.substring(0, bracketPos));
                }
                coordinate.add(Integer.valueOf(part.substring(bracketPos + 1, part.length() - 1)));
            } else {  // it's map access
                coordinate.add(part);
            }
        }
        this.coordinate = coordinate.toArray();
    }

    /**
     * generate coordinates for a list of addresses
     * @param address comma separated coordinates, each coordinate is a dot separated value
     *        address is like user.phoneNumber[0].areaCode,product.serialNum
     *        or in case of flat record, it can be userId,productId,serialNum
     * @return [[user,phoneNumber,0,areaCode],[product,serialNum] in case 1
     *        or [[userId],[productId],[serialNum]] in case 2
     *        or null if address is null
     */
    public static final FieldAddress[] generateFieldAddresses(String address) {
        if (address == null) {
            return null;
        }
        String[] addresses = address.substring(1,address.length()-1).split(","); // address is in form of [...], so take out outer [ and ]
        FieldAddress[] result = new FieldAddress[addresses.length];
        for (int i=0; i<addresses.length; i++) {  // transform each address to coordinate of form Object[]
            result[i] = new FieldAddress(addresses[i]);
        }
        return result;
    }

    public final Object[] getCoordinate() {return coordinate;}

    /**
     * convert address to string representation
     * for example,
     * [userId] becomes userId,
     * [userId,1] becomes userId[1]
     * [userId,part1,field1] becomes userId.part1.field1
     */
    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        for (Object s: coordinate) {
            if (s instanceof String) {
                sb.append(s).append(".");
            }
            else if (s instanceof Integer) {
                sb.append('[').append(((Integer)s).intValue()).append("]").append(".");
            }
        }
        sb.setLength(sb.length()-1);
        return sb.toString();
    }

}
