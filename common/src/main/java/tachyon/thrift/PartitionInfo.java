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
 * Created by zengdan on 15-11-12.
 */
public class PartitionInfo implements org.apache.thrift.TBase<PartitionInfo, PartitionInfo._Fields>, java.io.Serializable, Cloneable, Comparable<PartitionInfo> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("PartitionInfo");

    private static final org.apache.thrift.protocol.TField ID_FIELD_DESC = new org.apache.thrift.protocol.TField("id", TType.I32, (short)1);
    private static final org.apache.thrift.protocol.TField INDEX_FIELD_DESC = new org.apache.thrift.protocol.TField("index", TType.I32, (short)2);
    private static final org.apache.thrift.protocol.TField BENEFIT_FIELD_DESC = new org.apache.thrift.protocol.TField("benefit", TType.DOUBLE, (short)3);
    private static final org.apache.thrift.protocol.TField BLOCK_IDS_FIELD_DESC = new org.apache.thrift.protocol.TField("blocks", TType.LIST, (short)4);
    private static final org.apache.thrift.protocol.TField TIER_LEVEL_FIELD_DESC = new org.apache.thrift.protocol.TField("tierLevel", TType.I32, (short)5);
    private static final org.apache.thrift.protocol.TField DIR_INDEX_FIELD_DESC = new org.apache.thrift.protocol.TField("dirIndex", TType.I32, (short)6);
    private static final org.apache.thrift.protocol.TField BLOCK_SIZE_FIELD_DESC = new org.apache.thrift.protocol.TField("blockSize", TType.I64, (short)7);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
        schemes.put(StandardScheme.class, new PartitionInfoStandardSchemeFactory());
        schemes.put(TupleScheme.class, new PartitionInfoTupleSchemeFactory());
    }

    public int id;
    public int index;
    public double benefit;
    public List<Long> blockIds;
    public int tierLevel;
    public int dirIndex;
    public long blockSize;

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        ID((short)1, "id"),
        INDEX((short)2, "index"),
        BENEFIT((short)3, "benefit"),
        BLOCK_IDS((short)4, "blockIds"),
        TIER_LEVEL((short)5, "tierLevel"),
        DIR_INDEX((short)6, "dirIndex"),
        BLOCK_SIZE((short)7, "blockSize");

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
                    return ID;
                case 2:
                    return INDEX;
                case 3:
                    return BENEFIT;
                case 4:
                    return BLOCK_IDS;
                case 5:
                    return TIER_LEVEL;
                case 6:
                    return DIR_INDEX;
                case 7:
                    return BLOCK_SIZE;
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
    private static final int __ID_ISSET_ID = 0;
    private static final int __INDEX_ISSET_ID = 1;
    private static final int __BENEFIT_ISSET_ID = 2;
    private static final int __TIER_LEVEL_ISSET_ID = 3;
    private static final int __DIR_INDEX_ISSET_ID = 4;
    private static final int __BLOCK_SIZE_ISSET_ID = 5;
    private byte __isset_bitfield = 0;
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
        tmpMap.put(_Fields.ID, new org.apache.thrift.meta_data.FieldMetaData("id", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        tmpMap.put(_Fields.INDEX, new org.apache.thrift.meta_data.FieldMetaData("index", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        tmpMap.put(_Fields.BENEFIT, new org.apache.thrift.meta_data.FieldMetaData("benefit", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
        tmpMap.put(_Fields.BLOCK_IDS, new org.apache.thrift.meta_data.FieldMetaData("blockIds", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.ListMetaData(TType.LIST,
                        new org.apache.thrift.meta_data.FieldValueMetaData(TType.I64))));
        tmpMap.put(_Fields.TIER_LEVEL, new org.apache.thrift.meta_data.FieldMetaData("tierLevel", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        tmpMap.put(_Fields.DIR_INDEX, new org.apache.thrift.meta_data.FieldMetaData("dirIndex", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        tmpMap.put(_Fields.BLOCK_SIZE, new org.apache.thrift.meta_data.FieldMetaData("blockSize", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(PartitionInfo.class, metaDataMap);
    }

    public PartitionInfo() {
    }

    public PartitionInfo(int id, int index, double benefit) {
        this();
        this.id = id;
        setIdIsSet(true);
        this.index = index;
        setIndexIsSet(true);
        this.benefit = benefit;
        setBenefitIsSet(true);
        this.blockIds = new ArrayList<Long>();
        this.tierLevel = -1;
        setTierLevelIsSet(false);
        this.dirIndex = -1;
        setDirIndexIsSet(false);
        this.blockSize = 0;
        setBlockSizeIsSet(false);
    }

    public PartitionInfo(
            int id,
            int index,
            double benefit,
            List<Long> blockIds)
    {
        this();
        this.id = id;
        setIdIsSet(true);
        this.index = index;
        setIndexIsSet(true);
        this.benefit = benefit;
        setBenefitIsSet(true);
        this.blockIds = blockIds;
        this.tierLevel = -1;
        setTierLevelIsSet(false);
        this.dirIndex = -1;
        setDirIndexIsSet(false);
        this.blockSize = 0;
        setBlockSizeIsSet(false);
    }

    public PartitionInfo(
            int id,
            int index,
            double benefit,
            int tierLevel,
            int dirIndex,
            long blockSize)
    {
        this();
        this.id = id;
        setIdIsSet(true);
        this.index = index;
        setIndexIsSet(true);
        this.benefit = benefit;
        setBenefitIsSet(true);
        this.blockIds = new ArrayList<Long>();
        this.tierLevel = tierLevel;
        setTierLevelIsSet(true);
        this.dirIndex = dirIndex;
        setDirIndexIsSet(true);
        this.blockSize = blockSize;
        setBlockSizeIsSet(true);
    }

    public PartitionInfo(
            int id,
            int index,
            double benefit,
            int tierLevel,
            int dirIndex,
            long blockSize,
            List<Long> blockIds)
    {
        this();
        this.id = id;
        setIdIsSet(true);
        this.index = index;
        setIndexIsSet(true);
        this.benefit = benefit;
        setBenefitIsSet(true);
        this.blockIds = blockIds;
        this.tierLevel = tierLevel;
        setTierLevelIsSet(true);
        this.dirIndex = dirIndex;
        setDirIndexIsSet(true);
        this.blockSize = blockSize;
        setBlockSizeIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public PartitionInfo(PartitionInfo other) {
        __isset_bitfield = other.__isset_bitfield;
        this.id = other.id;
        this.index = other.index;
        this.benefit = other.benefit;
        this.tierLevel = other.tierLevel;
        this.dirIndex = other.dirIndex;
        this.blockSize = other.blockSize;
        if (other.isSetBlockIds()) {
            List<Long> __this__blockIds = new ArrayList<Long>(other.blockIds.size());
            for (Long other_element : other.blockIds) {
                __this__blockIds.add(other_element);
            }
            this.blockIds = __this__blockIds;
        }
    }

    public PartitionInfo deepCopy() {
        return new PartitionInfo(this);
    }

    @Override
    public void clear() {
        setIdIsSet(false);
        this.id = -1;
        setIndexIsSet(false);
        this.index = -1;
        setBenefitIsSet(false);
        this.benefit = 0;
        this.blockIds = null;
        setTierLevelIsSet(false);
        this.tierLevel = -1;
        setDirIndexIsSet(false);
        this.dirIndex = -1;
        this.blockSize = 0;
        setBlockSizeIsSet(false);
    }

    public int getId() {
        return this.id;
    }

    public PartitionInfo setId(int id) {
        this.id = id;
        setIdIsSet(true);
        return this;
    }

    public void unsetId() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ID_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetId() {
        return EncodingUtils.testBit(__isset_bitfield, __ID_ISSET_ID);
    }

    public void setIdIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ID_ISSET_ID, value);
    }

    public int getIndex() {
        return this.index;
    }

    public PartitionInfo setIndex(int index) {
        this.index = index;
        setIndexIsSet(true);
        return this;
    }

    public void unsetIndex() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __INDEX_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetIndex() {
        return EncodingUtils.testBit(__isset_bitfield, __INDEX_ISSET_ID);
    }

    public void setIndexIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __INDEX_ISSET_ID, value);
    }

    public double getBenefit() {
        return this.benefit;
    }

    public PartitionInfo setBenefit(double benefit) {
        this.benefit = benefit;
        setBenefitIsSet(true);
        return this;
    }

    public void unsetBenefit() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __BENEFIT_ISSET_ID);
    }

    /** Returns true if field offset is set (has been assigned a value) and false otherwise */
    public boolean isSetBenefit() {
        return EncodingUtils.testBit(__isset_bitfield, __BENEFIT_ISSET_ID);
    }

    public void setBenefitIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __BENEFIT_ISSET_ID, value);
    }

    public int getBlockIdsSize() {
        return (this.blockIds == null) ? 0 : this.blockIds.size();
    }

    public java.util.Iterator<Long> getBlockIdsIterator() {
        return (this.blockIds == null) ? null : this.blockIds.iterator();
    }

    public void addToBlockIds(long elem) {
        if (this.blockIds == null) {
            this.blockIds = new ArrayList<Long>();
        }
        this.blockIds.add(elem);
    }

    public List<Long> getBlockIds() {
        return this.blockIds;
    }

    public PartitionInfo setBlockIds(List<Long> blockIds) {
        this.blockIds = blockIds;
        return this;
    }

    public void unsetBlockIds() {
        this.blockIds = null;
    }

    /** Returns true if field locations is set (has been assigned a value) and false otherwise */
    public boolean isSetBlockIds() {
        return this.blockIds != null;
    }

    public void setBlockIdsIsSet(boolean value) {
        if (!value) {
            this.blockIds = null;
        }
    }

    public int getTierLevel() {
        return this.tierLevel;
    }

    public PartitionInfo setTierLevel(int tierLevel) {
        this.tierLevel = tierLevel;
        setTierLevelIsSet(true);
        return this;
    }

    public void unsetTierLevel() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TIER_LEVEL_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetTierLevel() {
        return EncodingUtils.testBit(__isset_bitfield, __TIER_LEVEL_ISSET_ID);
    }

    public void setTierLevelIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TIER_LEVEL_ISSET_ID, value);
    }

    public int getDirIndex() {
        return this.dirIndex;
    }

    public PartitionInfo setDirIndex(int dirIndex) {
        this.dirIndex = dirIndex;
        setDirIndexIsSet(true);
        return this;
    }

    public void unsetDirIndex() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __DIR_INDEX_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetDirIndex() {
        return EncodingUtils.testBit(__isset_bitfield, __DIR_INDEX_ISSET_ID);
    }

    public void setDirIndexIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __DIR_INDEX_ISSET_ID, value);
    }

    public long getBlockSize() {
        return this.blockSize;
    }

    public PartitionInfo setBlockSize(long blockSize) {
        this.blockSize = blockSize;
        setBlockSizeIsSet(true);
        return this;
    }

    public void unsetBlockSize() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __BLOCK_SIZE_ISSET_ID);
    }

    /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
    public boolean isSetBlockSize() {
        return EncodingUtils.testBit(__isset_bitfield, __BLOCK_SIZE_ISSET_ID);
    }

    public void setBlockSizeIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __BLOCK_SIZE_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
            case ID:
                if (value == null) {
                    unsetId();
                } else {
                    setId((Integer) value);
                }
                break;

            case INDEX:
                if (value == null) {
                    unsetIndex();
                } else {
                    setIndex((Integer) value);
                }
                break;

            case BENEFIT:
                if (value == null) {
                    unsetBenefit();
                } else {
                    setBenefit((Double) value);
                }
                break;

            case BLOCK_IDS:
                if (value == null) {
                    unsetBlockIds();
                } else {
                    setBlockIds((List<Long>)value);
                }
                break;
            case TIER_LEVEL:
                if (value == null) {
                    unsetTierLevel();
                } else {
                    setTierLevel((Integer) value);
                }
                break;
            case DIR_INDEX:
                if (value == null) {
                    unsetDirIndex();
                } else {
                    setDirIndex((Integer) value);
                }
                break;
            case BLOCK_SIZE:
                if (value == null) {
                    unsetBlockSize();
                } else {
                    setBlockSize((Long) value);
                }
                break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
            case ID:
                return Integer.valueOf(getId());

            case INDEX:
                return Integer.valueOf(getIndex());

            case BENEFIT:
                return Double.valueOf(getBenefit());

            case BLOCK_IDS:
                return getBlockIds();

            case TIER_LEVEL:
                return Integer.valueOf(getTierLevel());

            case DIR_INDEX:
                return Integer.valueOf(getDirIndex());

            case BLOCK_SIZE:
                return Long.valueOf(getBlockSize());

        }
        throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
            case ID:
                return isSetId();
            case INDEX:
                return isSetIndex();
            case BENEFIT:
                return isSetBenefit();
            case BLOCK_IDS:
                return isSetBlockIds();
            case TIER_LEVEL:
                return isSetTierLevel();
            case DIR_INDEX:
                return isSetDirIndex();
            case BLOCK_SIZE:
                return isSetBlockSize();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if (that == null)
            return false;
        if (that instanceof PartitionInfo)
            return this.equals((PartitionInfo)that);
        return false;
    }

    public boolean equals(PartitionInfo that) {
        if (that == null)
            return false;

        boolean this_present_id = true;
        boolean that_present_id = true;
        if (this_present_id || that_present_id) {
            if (!(this_present_id && that_present_id))
                return false;
            if (this.id != that.id)
                return false;
        }

        boolean this_present_index = true;
        boolean that_present_index = true;
        if (this_present_index || that_present_index) {
            if (!(this_present_index && that_present_index))
                return false;
            if (this.index != that.index)
                return false;
        }

        boolean this_present_benefit = true;
        boolean that_present_benefit = true;
        if (this_present_benefit || that_present_benefit) {
            if (!(this_present_benefit && that_present_benefit))
                return false;
        }

        boolean this_present_blockIds = true && this.isSetBlockIds();
        boolean that_present_blockIds = true && that.isSetBlockIds();
        if (this_present_blockIds || that_present_blockIds) {
            if (!(this_present_blockIds && that_present_blockIds))
                return false;
            //if (!this.blockIds.equals(that.blockIds))
            //    return false;
        }

        boolean this_present_tierLevel = true;
        boolean that_present_tierLevel = true;
        if (this_present_tierLevel || that_present_tierLevel) {
            if (!(this_present_tierLevel && that_present_tierLevel))
                return false;
        }

        boolean this_present_dirIndex = true;
        boolean that_present_dirIndex = true;
        if (this_present_dirIndex || that_present_dirIndex) {
            if (!(this_present_dirIndex && that_present_dirIndex))
                return false;
        }

        boolean this_present_blockSize = true;
        boolean that_present_blockSize = true;
        if (this_present_blockSize || that_present_blockSize) {
            if (!(this_present_blockSize && that_present_blockSize))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id + index;
    }

    @Override
    public int compareTo(PartitionInfo other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetId()).compareTo(other.isSetId());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetId()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.id, other.id);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        lastComparison = Boolean.valueOf(isSetIndex()).compareTo(other.isSetIndex());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetIndex()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.index, other.index);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }

        lastComparison = Boolean.valueOf(isSetBenefit()).compareTo(other.isSetBenefit());
        if (lastComparison != 0) {
            return lastComparison;
        }

        lastComparison = Boolean.valueOf(isSetBlockIds()).compareTo(other.isSetBlockIds());
        if (lastComparison != 0) {
            return lastComparison;
        }


        lastComparison = Boolean.valueOf(isSetTierLevel()).compareTo(other.isSetTierLevel());
        if (lastComparison != 0) {
            return lastComparison;
        }


        lastComparison = Boolean.valueOf(isSetDirIndex()).compareTo(other.isSetDirIndex());
        if (lastComparison != 0) {
            return lastComparison;
        }


        lastComparison = Boolean.valueOf(isSetBlockSize()).compareTo(other.isSetBlockSize());
        if (lastComparison != 0) {
            return lastComparison;
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
        StringBuilder sb = new StringBuilder("PartitionInfo(");
        boolean first = true;

        sb.append("id:");
        sb.append(this.id);
        first = false;
        if (!first) sb.append(", ");
        sb.append("index:");
        sb.append(this.index);
        first = false;
        if (!first) sb.append(", ");
        sb.append("benefit:");
        sb.append(this.benefit);
        first = false;
        if (!first) sb.append(", ");
        sb.append("blockIds:");
        if (this.blockIds == null) {
            sb.append("null");
        } else {
            sb.append(this.blockIds);
        }
        sb.append("tierLevel:");
        sb.append(this.tierLevel);
        first = false;
        if (!first) sb.append(", ");
        sb.append("dirIndex:");
        sb.append(this.dirIndex);
        first = false;
        if (!first) sb.append(", ");
        sb.append("blockSize:");
        sb.append(this.blockSize);
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

    private static class PartitionInfoStandardSchemeFactory implements SchemeFactory {
        public PartitionInfoStandardScheme getScheme() {
            return new PartitionInfoStandardScheme();
        }
    }

    private static class PartitionInfoStandardScheme extends StandardScheme<PartitionInfo> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, PartitionInfo struct) throws org.apache.thrift.TException {
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
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.id = iprot.readI32();
                            struct.setIdIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2: // INDEX
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.index = iprot.readI32();
                            struct.setIndexIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 3: // BENEFIT
                        if (schemeField.type == TType.DOUBLE) {
                            struct.benefit = iprot.readDouble();
                            struct.setBenefitIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;

                    case 4: // BLOCK_IDS
                        if (schemeField.type == TType.LIST) {
                            {
                                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                                struct.blockIds = new ArrayList<Long>(_list0.size);
                                for (int _i1 = 0; _i1 < _list0.size; ++_i1)
                                {
                                    long _elem2 = iprot.readI64();
                                    struct.blockIds.add(_elem2);
                                }
                                iprot.readListEnd();
                            }
                            struct.setBlockIdsIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 5:
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.tierLevel = iprot.readI32();
                            struct.setTierLevelIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 6:
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.dirIndex = iprot.readI32();
                            struct.setDirIndexIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 7:
                        if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
                            struct.blockSize = iprot.readI64();
                            struct.setBlockSizeIsSet(true);
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

        public void write(org.apache.thrift.protocol.TProtocol oprot, PartitionInfo struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            oprot.writeFieldBegin(ID_FIELD_DESC);
            oprot.writeI32(struct.id);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(INDEX_FIELD_DESC);
            oprot.writeI32(struct.index);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(BENEFIT_FIELD_DESC);
            oprot.writeDouble(struct.benefit);
            oprot.writeFieldEnd();
            if (struct.blockIds != null) {
                oprot.writeFieldBegin(BLOCK_IDS_FIELD_DESC);
                {
                    oprot.writeListBegin(new org.apache.thrift.protocol.TList(TType.I64, struct.blockIds.size()));
                    for (long _iter3 : struct.blockIds)
                    {
                        oprot.writeI64(_iter3);
                    }
                    oprot.writeListEnd();
                }
                oprot.writeFieldEnd();
            }
            oprot.writeFieldBegin(TIER_LEVEL_FIELD_DESC);
            oprot.writeI32(struct.tierLevel);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(DIR_INDEX_FIELD_DESC);
            oprot.writeI32(struct.dirIndex);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(BLOCK_SIZE_FIELD_DESC);
            oprot.writeI64(struct.blockSize);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class PartitionInfoTupleSchemeFactory implements SchemeFactory {
        public PartitionInfoTupleScheme getScheme() {
            return new PartitionInfoTupleScheme();
        }
    }

    private static class PartitionInfoTupleScheme extends TupleScheme<PartitionInfo> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, PartitionInfo struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            BitSet optionals = new BitSet();
            if (struct.isSetId()) {
                optionals.set(0);
            }
            if (struct.isSetIndex()) {
                optionals.set(1);
            }
            if (struct.isSetBenefit()) {
                optionals.set(2);
            }
            if (struct.isSetBlockIds()) {
                optionals.set(3);
            }
            if (struct.isSetTierLevel()) {
                optionals.set(4);
            }
            if (struct.isSetDirIndex()) {
                optionals.set(5);
            }
            if (struct.isSetBlockSize()) {
                optionals.set(6);
            }
            oprot.writeBitSet(optionals, 7);
            if (struct.isSetId()) {
                oprot.writeI32(struct.id);
            }
            if (struct.isSetIndex()) {
                oprot.writeI32(struct.index);
            }
            if (struct.isSetBenefit()) {
                oprot.writeDouble(struct.benefit);
            }
            if (struct.isSetBlockIds()) {
                {
                    oprot.writeI32(struct.blockIds.size());
                    for (long _iter4 : struct.blockIds)
                    {
                        oprot.writeI64(_iter4);
                    }
                }
            }
            if (struct.isSetTierLevel()) {
                oprot.writeI32(struct.tierLevel);
            }
            if (struct.isSetDirIndex()) {
                oprot.writeI32(struct.dirIndex);
            }
            if (struct.isSetBlockSize()) {
                oprot.writeI64(struct.blockSize);
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, PartitionInfo struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            BitSet incoming = iprot.readBitSet(7);
            if (incoming.get(0)) {
                struct.id = iprot.readI32();
                struct.setIdIsSet(true);
            }
            if (incoming.get(1)) {
                struct.index = iprot.readI32();
                struct.setIndexIsSet(true);
            }
            if (incoming.get(2)) {
                struct.benefit = iprot.readDouble();
                struct.setBenefitIsSet(true);
            }
            if (incoming.get(3)) {
                {
                    org.apache.thrift.protocol.TList _list5 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
                    struct.blockIds = new ArrayList<Long>(_list5.size);
                    for (int _i6 = 0; _i6 < _list5.size; ++_i6)
                    {
                        long _elem7 = iprot.readI64();
                        struct.blockIds.add(_elem7);
                    }
                }
                struct.setBlockIdsIsSet(true);
            }
            if (incoming.get(4)) {
                struct.tierLevel = iprot.readI32();
                struct.setTierLevelIsSet(true);
            }
            if (incoming.get(5)) {
                struct.dirIndex = iprot.readI32();
                struct.setDirIndexIsSet(true);
            }
            if (incoming.get(6)) {
                struct.blockSize = iprot.readI64();
                struct.setBlockSizeIsSet(true);
            }
        }
    }
}
