package tachyon.thrift;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.util.*;

/**
 * Created by zengdan on 15-12-30.
 */
public class BenefitInfo implements org.apache.thrift.TBase<BenefitInfo, BenefitInfo._Fields>, java.io.Serializable, Cloneable, Comparable<BenefitInfo> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("BenefitInfo");

    private static final org.apache.thrift.protocol.TField RECENCY_FIELD_DESC = new org.apache.thrift.protocol.TField("recency", TType.I64, (short)1);
    private static final org.apache.thrift.protocol.TField REF_FIELD_DESC = new org.apache.thrift.protocol.TField("ref", TType.I64, (short)2);
    private static final org.apache.thrift.protocol.TField COST_FIELD_DESC = new org.apache.thrift.protocol.TField("cost", TType.I64, (short)3);
    private static final org.apache.thrift.protocol.TField DATA_SIZE_FIELD_DESC = new org.apache.thrift.protocol.TField("dataSize", TType.I64, (short)4);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
        schemes.put(StandardScheme.class, new BenefitInfoStandardSchemeFactory());
        schemes.put(TupleScheme.class, new BenefitInfoTupleSchemeFactory());
    }

    public long recency;
    public long ref;
    public long cost;
    public long dataSize;

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        RECENCY((short)1, "recency"),
        REF((short)2, "ref"),
        COST((short)3, "cost"),
        DATA_SIZE((short)4, "dataSize");

        private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

        static {
            for (_Fields field : EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not found.
         */
        public static _Fields findByThriftId(int fieldId) {
            switch(fieldId) {
                case 1:
                    return RECENCY;
                case 2:
                    return REF;
                case 3:
                    return COST;
                case 4:
                    return DATA_SIZE;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId) {
            _Fields fields = findByThriftId(fieldId);
            if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        public static _Fields findByName(String name) {
            return byName.get(name);
        }

        private final short _thriftId;
        private final String _fieldName;

        _Fields(short thriftId, String fieldName) {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        public short getThriftFieldId() {
            return _thriftId;
        }

        public String getFieldName() {
            return _fieldName;
        }
    }

    // isset id assignments
    private static final int __RECENCY_ISSET_ID = 0;
    private static final int __REF_ISSET_ID = 1;
    private static final int __COST_ISSET_ID = 2;
    private static final int __DATA_SIZE_ISSET_ID = 3;
    private byte __isset_bitfield = 0;
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
        tmpMap.put(_Fields.RECENCY, new org.apache.thrift.meta_data.FieldMetaData("recency", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
        tmpMap.put(_Fields.REF, new org.apache.thrift.meta_data.FieldMetaData("ref", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
        tmpMap.put(_Fields.COST, new org.apache.thrift.meta_data.FieldMetaData("cost", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
        tmpMap.put(_Fields.DATA_SIZE, new org.apache.thrift.meta_data.FieldMetaData("dataSize", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(BenefitInfo.class, metaDataMap);
    }

    public BenefitInfo() {
    }

    public BenefitInfo(long recency, long ref, long cost, long dataSize) {
        this();
        this.recency = recency;
        setRecencyIsSet(true);
        this.ref = ref;
        setRefIsSet(true);
        this.cost = cost;
        setCostIsSet(true);
        this.dataSize = dataSize;
        setDataSizeIsSet(true);
    }
    /**
     * Performs a deep copy on <i>other</i>.
     */
    public BenefitInfo(BenefitInfo other) {
        __isset_bitfield = other.__isset_bitfield;
        this.recency = other.recency;
        this.ref = other.ref;
        this.cost = other.cost;
        this.dataSize = other.dataSize;
    }

    public BenefitInfo deepCopy() {
        return new BenefitInfo(this);
    }

    @Override
    public void clear() {
        setRecencyIsSet(false);
        this.recency = 0;
        setRefIsSet(false);
        this.ref = 0;
        setCostIsSet(false);
        this.cost = 0;
        setDataSizeIsSet(false);
        this.dataSize = 0;
    }

    public long getRecency() {
        return this.recency;
    }

    public BenefitInfo setRecency(long recency) {
        this.recency = recency;
        setRecencyIsSet(true);
        return this;
    }

    public void unsetRecency() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __RECENCY_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetRecency() {
        return EncodingUtils.testBit(__isset_bitfield, __RECENCY_ISSET_ID);
    }

    public void setRecencyIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __RECENCY_ISSET_ID, value);
    }

    public long getRef() {
        return this.ref;
    }

    public BenefitInfo setRef(long ref) {
        this.ref = ref;
        setRefIsSet(true);
        return this;
    }

    public void unsetRef() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __REF_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetRef() {
        return EncodingUtils.testBit(__isset_bitfield, __REF_ISSET_ID);
    }

    public void setRefIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __REF_ISSET_ID, value);
    }

    public long getCost() {
        return this.cost;
    }

    public BenefitInfo setCost(long cost) {
        this.cost = cost;
        setCostIsSet(true);
        return this;
    }

    public void unsetCost() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __COST_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetCost() {
        return EncodingUtils.testBit(__isset_bitfield, __COST_ISSET_ID);
    }

    public void setCostIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __COST_ISSET_ID, value);
    }

    public long getDataSize() {
        return this.dataSize;
    }

    public BenefitInfo setDataSize(long dataSize) {
        this.dataSize = dataSize;
        setDataSizeIsSet(true);
        return this;
    }

    public void unsetDataSize() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __DATA_SIZE_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetDataSize() {
        return EncodingUtils.testBit(__isset_bitfield, __DATA_SIZE_ISSET_ID);
    }

    public void setDataSizeIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __DATA_SIZE_ISSET_ID, value);
    }



    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
            case RECENCY:
                if (value == null) {
                    unsetRecency();
                } else {
                    setRecency((Long) value);
                }
                break;

            case REF:
                if (value == null) {
                    unsetRef();
                } else {
                    setRef((Long) value);
                }
                break;

            case COST:
                if (value == null) {
                    unsetCost();
                } else {
                    setCost((Long) value);
                }
                break;

            case DATA_SIZE:
                if (value == null) {
                    unsetDataSize();
                } else {
                    setDataSize((Long) value);
                }
                break;
        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
            case RECENCY:
                return Long.valueOf(getRecency());

            case REF:
                return Long.valueOf(getRef());

            case COST:
                return Long.valueOf(getCost());

            case DATA_SIZE:
                return Long.valueOf(getDataSize());

        }
        throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
            case RECENCY:
                return isSetRecency();
            case REF:
                return isSetRef();
            case COST:
                return isSetCost();
            case DATA_SIZE:
                return isSetDataSize();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if (that == null)
            return false;
        if (that instanceof BenefitInfo)
            return this.equals((BenefitInfo)that);
        return false;
    }

    public boolean equals(BenefitInfo that) {
        if (that == null)
            return false;

        boolean this_present_recency = true;
        boolean that_present_recency = true;
        if (this_present_recency || that_present_recency) {
            if (!(this_present_recency && that_present_recency))
                return false;
            if (this.recency != that.recency)
                return false;
        }

        boolean this_present_ref = true;
        boolean that_present_ref = true;
        if (this_present_ref || that_present_ref) {
            if (!(this_present_ref && that_present_ref))
                return false;
            if (this.ref != that.ref)
                return false;
        }

        boolean this_present_cost = true;
        boolean that_present_cost = true;
        if (this_present_cost || that_present_cost) {
            if (!(this_present_cost && that_present_cost))
                return false;
            if (this.cost != that.cost)
                return false;
        }

        boolean this_present_dataSize = true;
        boolean that_present_dataSize = true;
        if (this_present_dataSize || that_present_dataSize) {
            if (!(this_present_dataSize && that_present_dataSize))
                return false;
            if (this.dataSize != that.dataSize)
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public int compareTo(BenefitInfo other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetRecency()).compareTo(other.isSetRecency());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetRecency()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.recency, other.recency);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        lastComparison = Boolean.valueOf(isSetRef()).compareTo(other.isSetRef());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetRef()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ref, other.ref);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        lastComparison = Boolean.valueOf(isSetCost()).compareTo(other.isSetCost());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetCost()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.cost, other.cost);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        lastComparison = Boolean.valueOf(isSetDataSize()).compareTo(other.isSetDataSize());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetDataSize()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dataSize, other.dataSize);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        return 0;
    }

    public _Fields fieldForId(int fieldId) {
        return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
        schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
        schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("BenefitInfo(");
        boolean first = true;
        sb.append("recency:");
        sb.append(this.recency);
        first = false;
        if (!first) sb.append(", ");
        sb.append("ref:");
        sb.append(this.ref);
        first = false;
        if (!first) sb.append(", ");
        sb.append("cost:");
        sb.append(this.cost);
        first = false;
        if (!first) sb.append(", ");
        sb.append("dataSize:");
        sb.append(this.dataSize);
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        try {
            write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        try {
            // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
            __isset_bitfield = 0;
            read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class BenefitInfoStandardSchemeFactory implements SchemeFactory {
        public BenefitInfoStandardScheme getScheme() {
            return new BenefitInfoStandardScheme();
        }
    }

    private static class BenefitInfoStandardScheme extends StandardScheme<BenefitInfo> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, BenefitInfo struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true)
            {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // ID
                        if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
                            struct.recency = iprot.readI64();
                            struct.setRecencyIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2:
                        if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
                            struct.ref = iprot.readI64();
                            struct.setRefIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 3:
                        if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
                            struct.cost = iprot.readI64();
                            struct.setCostIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 4:
                        if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
                            struct.dataSize = iprot.readI64();
                            struct.setDataSizeIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    default:
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // check for required fields of primitive type, which can't be checked in the validate method
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot, BenefitInfo struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            oprot.writeFieldBegin(RECENCY_FIELD_DESC);
            oprot.writeI64(struct.recency);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(REF_FIELD_DESC);
            oprot.writeI64(struct.ref);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(COST_FIELD_DESC);
            oprot.writeI64(struct.cost);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(DATA_SIZE_FIELD_DESC);
            oprot.writeI64(struct.dataSize);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class BenefitInfoTupleSchemeFactory implements SchemeFactory {
        public BenefitInfoTupleScheme getScheme() {
            return new BenefitInfoTupleScheme();
        }
    }

    private static class BenefitInfoTupleScheme extends TupleScheme<BenefitInfo> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, BenefitInfo struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            BitSet optionals = new BitSet();
            if (struct.isSetRecency()) {
                optionals.set(0);
            }
            if (struct.isSetRef()) {
                optionals.set(1);
            }
            if (struct.isSetCost()) {
                optionals.set(2);
            }
            if (struct.isSetDataSize()) {
                optionals.set(3);
            }
            oprot.writeBitSet(optionals, 4);
            if (struct.isSetRecency()) {
                oprot.writeI64(struct.recency);
            }
            if (struct.isSetRef()) {
                oprot.writeI64(struct.ref);
            }
            if (struct.isSetCost()) {
                oprot.writeI64(struct.cost);
            }
            if (struct.isSetDataSize()) {
                oprot.writeI64(struct.dataSize);
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, BenefitInfo struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            BitSet incoming = iprot.readBitSet(4);
            if (incoming.get(0)) {
                struct.recency = iprot.readI64();
                struct.setRecencyIsSet(true);
            }
            if (incoming.get(1)) {
                struct.ref = iprot.readI64();
                struct.setRefIsSet(true);
            }
            if (incoming.get(2)) {
                struct.cost = iprot.readI64();
                struct.setCostIsSet(true);
            }
            if (incoming.get(3)) {
                struct.dataSize = iprot.readI64();
                struct.setDataSizeIsSet(true);
            }
        }
    }
}

