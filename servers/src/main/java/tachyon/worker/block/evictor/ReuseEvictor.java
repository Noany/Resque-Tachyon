/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block.evictor;

/**
 * Created by zengdan on 15-11-9.
 */

import java.io.IOException;
import java.util.*;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.StorageLevelAlias;
import tachyon.thrift.BenefitInfo;
import tachyon.thrift.PartitionInfo;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.PartitionInfoByStorage;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;

public class ReuseEvictor extends BlockStoreEventListenerBase implements Evictor {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMetadataManagerView mManagerView;
  private int memBandwidth;
  private int diskBandwidth;
  //private final Map<Pair<Integer, Integer>, Set<PartitionInfo>> mBlockInfos =
  //        new HashMap<Pair<Integer, Integer>, Set<PartitionInfo>>();
  private final Set<Pair<PartitionInfo, Double>> mPartitions =
          new TreeSet<Pair<PartitionInfo, Double>>(new Comparator<Pair<PartitionInfo, Double>>() {
            @Override
            public int compare(Pair<PartitionInfo, Double> o1, Pair<PartitionInfo, Double> o2) {
              return (int)(o1.getSecond() - o2.getSecond());
            }
          });

  //zengdan
  public double computeBenefit(PartitionInfo partition) {
    long firstBlock = partition.blockIds.get(0);

    try {
      int tierAlias = mManagerView.getBlockMeta(firstBlock).getBlockLocation().tierAlias();
      int bandwidth = 0;
      if (tierAlias == StorageLevelAlias.MEM.getValue()) {
        bandwidth = memBandwidth;
      } else if(tierAlias == StorageLevelAlias.HDD.getValue()) {
        bandwidth = diskBandwidth;
      } else {
        return 0;
      }
      BenefitInfo benefitInfo = partition.getBenefit();
      return benefitInfo.getRecency()*(benefitInfo.getRef()*benefitInfo.getCost()*1.0/1000 -
              (benefitInfo.getRef() + 1)*benefitInfo.getDataSize()*1.0/(1000000*bandwidth));
    } catch (IOException e) {
      LOG.error("Failed to computeBenefit for partition " + partition + " because blockId " + firstBlock + " not exists");
      return 0;
    }
  }

  //zengdan for not stored partition
  public double computeBenefit(BenefitInfo benefitInfo, boolean inMem) {
    int bandwidth;
    if (inMem) {
      bandwidth = memBandwidth;
    } else {
      bandwidth = diskBandwidth;
    }
    return benefitInfo.getRecency()*(benefitInfo.getRef()*benefitInfo.getCost()*1.0/1000 -
            (benefitInfo.getRef() + 1)*benefitInfo.getDataSize()*1.0/(1000000*bandwidth));
  }


  public ReuseEvictor(BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view);
  }

  @Override
  public void setBandwidth(int memBandwidth, int diskBandwidth) {
    this.memBandwidth = memBandwidth;
    this.diskBandwidth = diskBandwidth;
  }

  @Override
  public EvictionPlan freeSpaceWithView(long availableBytes, BlockStoreLocation location,
      BlockMetadataManagerView view) throws IOException {
    mManagerView = view;
    LOG.info("Free space using ReuseEvictor.");
    return freeSpace(availableBytes, location);
  }

  //zengdan
  @Override
  public EvictionPlan freePartitionSpaceWithView(BenefitInfo benefitInfo, long totalSize,
      List<BlockStoreLocation> locations, BlockMetadataManagerView view) throws IOException {
    getLocalBlocksInfos();
    LOG.info("In ReuseEvitor: FreePartitionSpace for benefit {}, size {}", benefitInfo, totalSize);
    List<Pair<Long, BlockStoreLocation>> toTransfer = new java.util.ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    List<PartitionInfo> reuseVictims = new ArrayList<PartitionInfo>();
    List<BlockMeta> ordinaryVictims = new ArrayList<BlockMeta>();

    StorageDirView selectedDirView = null;

    for (BlockStoreLocation location : locations) {
      if (location.equals(BlockStoreLocation.anyTier())) {
        selectedDirView = selectDirToEvictBlocksFromAnyTier(totalSize, benefitInfo, reuseVictims, ordinaryVictims);
      } else {
        int tierAlias = location.tierAlias();
        StorageTierView tierView = mManagerView.getTierView(tierAlias);
        if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
          selectedDirView = selectDirToEvictBlocksFromTier(tierView, totalSize, benefitInfo, reuseVictims, ordinaryVictims);
        } else {
          int dirIndex = location.dir();
          StorageDirView dir = tierView.getDirView(dirIndex);
          double benefit = computeBenefit(benefitInfo, tierView.getTierViewAlias() == StorageLevelAlias.MEM.getValue());
          if (canEvictBlocksFromDir(dir, totalSize, benefit, reuseVictims, ordinaryVictims)) {
            selectedDirView = dir;
          }
        }
      }
      if (selectedDirView != null) {
        break;
      }
    }
    if (selectedDirView == null) {
      LOG.error("Failed to freeSpace: No StorageDirView has enough capacity of {} bytes",
              totalSize);
      return null;
    }

    long bytesAvailableInDir = selectedDirView.getAvailableBytes();
    if (bytesAvailableInDir < totalSize) {
      Map<StorageDirView, Long> pendingBytesInDir = new HashMap<StorageDirView, Long>();
      transferBlocks(reuseVictims, ordinaryVictims, toEvict, toTransfer, pendingBytesInDir);
    }

    return new EvictionPlan(toTransfer, toEvict);
  }



  void transferBlocks(List<PartitionInfo> reuseVictims, List<BlockMeta> ordinaryVictims,
                      List<Long> toEvict, List<Pair<Long, BlockStoreLocation>> toTransfer,
                      Map<StorageDirView, Long> pendingBytesInDir) throws IOException {
    if (reuseVictims.isEmpty() && ordinaryVictims.isEmpty()) {
      return;
    }
    //Set<Pair<BlockMeta, Double>> nextVictims = new HashSet<Pair<BlockMeta, Double>>();
    List<PartitionInfo> nextReuseVictims = new ArrayList<PartitionInfo>();
    List<BlockMeta> nextOrdinaryVictims = new ArrayList<BlockMeta>();
    for (PartitionInfo partition : reuseVictims) {
      List<PartitionInfo> curNextReuseVictims = new ArrayList<PartitionInfo>();
      List<BlockMeta> curNextOrdinaryVictims = new ArrayList<BlockMeta>();
      List<StorageTierView> toTiers = mManagerView.getTierViewsBelowLevel(partition.getTierLevel());

      StorageDirView toDir = selectDirToTransferPartition(partition.getBenefit(),
              partition.getBlockSize(), toTiers,
              pendingBytesInDir, curNextReuseVictims, curNextOrdinaryVictims);
      //*partition.getBlockIds().size()
      if (toDir == null) {
        toEvict.addAll(partition.getBlockIds());
        LOG.info("Evict partition " + partition);
      } else {
        StorageTierView toTier = toDir.getParentTierView();
        for (long blockId : partition.getBlockIds()) {
          BlockMeta block = mManagerView.getBlockMeta(blockId);
          toTransfer.add(new Pair<Long, BlockStoreLocation>(blockId,
                  new BlockStoreLocation(toTier.getTierViewAlias(), toTier.getTierViewLevel(),
                          toDir.getDirViewIndex())));
          if (pendingBytesInDir.containsKey(toDir)) {
            pendingBytesInDir.put(toDir, pendingBytesInDir.get(toDir) + block.getBlockSize());
          } else {
            pendingBytesInDir.put(toDir, block.getBlockSize());
          }
        }
        LOG.info("Transfer partition {} to tierLevel {}", partition, toTier.getTierViewLevel());
        nextReuseVictims.addAll(curNextReuseVictims);
        nextOrdinaryVictims.addAll(curNextOrdinaryVictims);
      }
    }

    for (BlockMeta block : ordinaryVictims) {
      int fromTierAlias = block.getParentDir().getParentTier().getTierAlias();
      List<StorageTierView> toTiers = mManagerView.getTierViewsBelow(fromTierAlias);
      List<PartitionInfo> curNextReuseVictims = new ArrayList<PartitionInfo>();
      List<BlockMeta> curNextOrdinaryVictims = new ArrayList<BlockMeta>();
      StorageDirView toDir = selectDirToTransferBlock(block.getBlockSize(), toTiers,
              pendingBytesInDir, curNextReuseVictims, curNextOrdinaryVictims);
      if (toDir == null) {
        // Not possible to transfer
        toEvict.add(block.getBlockId());
        LOG.debug("Evict block " + block.getBlockId());
      } else {
        StorageTierView toTier = toDir.getParentTierView();
        toTransfer.add(new Pair<Long, BlockStoreLocation>(block.getBlockId(),
                new BlockStoreLocation(toTier.getTierViewAlias(), toTier.getTierViewLevel(),
                        toDir.getDirViewIndex())));
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytesInDir.put(toDir, pendingBytesInDir.get(toDir) + block.getBlockSize());
        } else {
          pendingBytesInDir.put(toDir, block.getBlockSize());
        }
        LOG.debug("Transfer block " + block.getBlockId());
        nextReuseVictims.addAll(curNextReuseVictims);
        nextOrdinaryVictims.addAll(curNextOrdinaryVictims);
      }
    }

    transferBlocks(nextReuseVictims, nextOrdinaryVictims, toEvict, toTransfer, pendingBytesInDir);
  }



  //To do: Synchronized
  // Now assume that the blocks newly entered in needn't to be evicted
  public void getLocalBlocksInfos() {
    //mBlockInfos.clear();
    mPartitions.clear();
    Set<PartitionInfo> partitions = mManagerView.getBlockIdToBenefit();
    LOG.info("Got {} partitions in ReuseEvictor.", partitions.size());
    for (PartitionInfo partition : partitions) {
      BlockMeta meta = null;
      for (long blockId : partition.getBlockIds()) {
        try {
          meta = mManagerView.getBlockMeta(blockId);
        } catch (IOException e) {
          LOG.debug(String.format("Block %d not exists on local worker!", blockId));
        }
        if (meta == null) {
          break;
        }
      }
      if (meta != null) {
        //partition.setBlockSize(meta.getBlockSize());
        BlockStoreLocation location = meta.getBlockLocation();
        partition.setTierLevel(location.tierLevel());
        partition.setDirIndex(location.dir());
        LOG.debug("Added partition " + partition + " to mPartitions.");
        boolean inMem = location.tierAlias() == StorageLevelAlias.MEM.getValue();
        mPartitions.add(new Pair<PartitionInfo, Double>(partition, computeBenefit(partition.getBenefit(), inMem)));
      }
    }
    LOG.info("Added {} partitions to mPartitions.", mPartitions.size());

    /*
    //List<PartitionInfoByStorage> storages = new ArrayList<PartitionInfoByStorage>();
    for (PartitionInfo partitionInfo : partitions) {
      int id = partitionInfo.getId();
      double benefit = partitionInfo.getBenefit();
      PartitionInfoByStorage storageMeta = new PartitionInfoByStorage(id, benefit);
      for (long blockId: partitionInfo.getBlockIds()) {
        //group blocks by storage dir in internal partition
        BlockMeta meta = null;
        try {
          meta = mManagerView.getBlockMeta(blockId);
        } catch (IOException e) {
          LOG.error(String.format("Block %d not exists on local worker!", blockId));
        }
        if (meta != null) {
          BlockStoreLocation location = meta.getBlockLocation();
          int tierLevel = location.tierLevel();
          int dirIndex = location.dir(); //dirIndex
          storageMeta.addBlockMeta(tierLevel, dirIndex, id);
        }
      }
      if (!storageMeta.getBlockIds().isEmpty()) {
        partitionsByStorage.add(storageMeta);
      }
    }

    /*
    for (PartitionInfoByStorage storage : storages) {
      for (Map.Entry<Pair<Integer, Integer>, Set<Long>> entry : storage.getBlockIds().entrySet()) {
        Pair<Integer, Integer> key = entry.getKey();
        if (mBlockInfos.get(key) == null) {
          mBlockInfos.put(key, new TreeSet<PartitionInfo>(new Comparator<PartitionInfo>() {
            @Override
            public int compare(PartitionInfo o1, PartitionInfo o2) {
              return (int)(o1.getBenefit() - o2.getBenefit());
            }
          }));
        }
        mBlockInfos.get(key).add(new PartitionInfo(storage.getId(), storage.getBenefit(), entry.getValue()));
      }
    }
    */

  }



  boolean canEvictBlocksFromDir(StorageDirView dirView, long availableBytes, double benefit,
       List<PartitionInfo> reuseVictims, List<BlockMeta> ordinaryVictims) {
    int tierLevel = dirView.getParentTierView().getTierViewLevel();
    int dirIndex = dirView.getDirViewIndex();


    LOG.debug("Test candidate dirView with tierLevel {}, dirIndex {} and {} capacityBytes, {} available bytes",
            tierLevel, dirIndex, dirView.getCapacityBytes(), dirView.getAvailableBytes());
    LOG.debug("There is {} partitions on this worker.", mPartitions.size());
    long requiredBytes = availableBytes - dirView.getAvailableBytes();
    List<BlockMeta> allBlocks = dirView.getEvictableBlocks();
    for (Pair<PartitionInfo, Double> partitionWithBenefit : mPartitions) {
      if (requiredBytes <= 0) {
        return true;
      }
      PartitionInfo partition = partitionWithBenefit.getFirst();
      if (partition.getTierLevel() != tierLevel || partition.getDirIndex() != dirIndex) {
        continue;
      }
      LOG.debug("Current candidate partition: " + partition);
      if (partitionWithBenefit.getSecond() >= benefit) {
        return false;
      }
      boolean evictPartition = false;
      for (long blockId : partition.getBlockIds()) {
        if (mManagerView.isBlockEvictable(blockId)) {
          BlockMeta meta = null;
          try {
            meta = mManagerView.getBlockMeta(blockId);
          } catch (IOException e) {
            LOG.error(String.format("Block %d not exists on local worker!", blockId));
          }
          if (meta != null) {
            requiredBytes -= meta.getBlockSize();
            //victims.add(new Pair<BlockMeta, Double>(meta, storage.getBenefit()));
            allBlocks.remove(meta);
            evictPartition = true;
          }
        }
      }

      if (evictPartition) { //remove other blocks of the partition that belongs to different storage position
        LOG.debug("Victim partition " + partition);
        reuseVictims.add(partition);
      }
    }

    //evict other blocks that doesn't belong to our reuse storage
    /*
    in case of partition info not immediately gotten
    for (BlockMeta block : allBlocks) {
      if (requiredBytes <= 0) {
        return true;
      }
      requiredBytes -= block.getBlockSize();
      ordinaryVictims.add(block);
      LOG.info("Victim block " + block.getBlockId());
    }
    */
    if (requiredBytes > 0) {
      reuseVictims.clear();
      ordinaryVictims.clear();
      return false;
    } else {
      return true;
    }
    //false  victims is empty
    //true  victims can either be empty or not empty
  }



  boolean canEvictBlocksFromDir(StorageDirView dirView, long availableBytes, double benefit,
                                Map<StorageDirView, Long> pendingBytesInDir,
                                List<PartitionInfo> reuseVictims, List<BlockMeta> ordinaryVictims) {
    long pendingBytes = 0L;
    if (pendingBytesInDir.containsKey(dirView)) {
      pendingBytes = pendingBytesInDir.get(dirView);
    }
    availableBytes += pendingBytes;
    return canEvictBlocksFromDir(dirView, availableBytes, benefit, reuseVictims, ordinaryVictims);
  }



  private StorageDirView selectDirToEvictBlocksFromAnyTier(long availableBytes, BenefitInfo benefitInfo,
       List<PartitionInfo> reuseVictims, List<BlockMeta> ordinaryVictims) {
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      StorageDirView dirView =  selectDirToEvictBlocksFromTier(tierView, availableBytes, benefitInfo, reuseVictims,
              ordinaryVictims);
      if (dirView != null) {
        return dirView;
      }
    }
    return null;
  }

  private StorageDirView selectDirToEvictBlocksFromTier(StorageTierView tierView,
       long availableBytes, BenefitInfo benefitInfo, List<PartitionInfo> reuseVictims,
       List<BlockMeta> ordinaryVictims) {
    double benefit = computeBenefit(benefitInfo, tierView.getTierViewAlias() == StorageLevelAlias.MEM.getValue());
    for (StorageDirView dirView : tierView.getDirViews()) {
      if (dirView.getAvailableBytes() >= availableBytes) {
        return dirView;
      }
      if (canEvictBlocksFromDir(dirView, availableBytes, benefit, reuseVictims, ordinaryVictims)) {
        return dirView;
      }
    }
    return null;
  }



  private StorageDirView selectDirToTransferPartition(BenefitInfo benefitInfoInfo, long totalSize,
          List<StorageTierView> toTiers, Map<StorageDirView, Long> pendingBytesInDir,
          List<PartitionInfo> reuseVictims, List<BlockMeta> ordinaryVictims) {
    for (StorageTierView toTier : toTiers) {
      double benefit = computeBenefit(benefitInfoInfo, toTier.getTierViewAlias() == StorageLevelAlias.MEM.getValue());
      for (StorageDirView toDir: toTier.getDirViews()) {
        long pendingBytes = 0L;
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytes = pendingBytesInDir.get(toDir);
        }
        if (toDir.getAvailableBytes() - pendingBytes >= totalSize) {
          return toDir;
        }
        if (canEvictBlocksFromDir(toDir, totalSize, benefit,
                pendingBytesInDir, reuseVictims, ordinaryVictims)) {
          return toDir;
        }
      }
    }
    return null;
  }

  private StorageDirView selectDirToTransferBlock(long blockSize,
       List<StorageTierView> toTiers, Map<StorageDirView, Long> pendingBytesInDir,
       List<PartitionInfo> reuseVictims, List<BlockMeta> ordinaryVictims) {
    for (StorageTierView toTier : toTiers) {
      for (StorageDirView toDir: toTier.getDirViews()) {
        long pendingBytes = 0L;
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytes = pendingBytesInDir.get(toDir);
        }
        if (toDir.getAvailableBytes() - pendingBytes >= blockSize) {
          return toDir;
        }
        if (canEvictBlocksFromDir(toDir, blockSize, Double.MAX_VALUE,
                pendingBytesInDir, reuseVictims, ordinaryVictims)) {
          return toDir;
        }
      }
    }
    return null;
  }

  private EvictionPlan freeSpace(long availableBytes, BlockStoreLocation location)
          throws IOException {
    // 1. Select a StorageDirView that has enough capacity for required bytes.
    StorageDirView selectedDirView = null;
    if (location.equals(BlockStoreLocation.anyTier())) {
      selectedDirView = selectDirToEvictBlocksFromAnyTier(availableBytes);
    } else {
      int tierAlias = location.tierAlias();
      StorageTierView tierView = mManagerView.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        selectedDirView = selectDirToEvictBlocksFromTier(tierView, availableBytes);
      } else {
        int dirIndex = location.dir();
        StorageDirView dir = tierView.getDirView(dirIndex);
        if (canEvictBlocksFromDir(dir, availableBytes)) {
          selectedDirView = dir;
        }
      }
    }
    if (selectedDirView == null) {
      LOG.error("Failed to freeSpace: No StorageDirView has enough capacity of {} bytes",
              availableBytes);
      return null;
    }

    // 2. Check if the selected StorageDirView already has enough space.
    List<Pair<Long, BlockStoreLocation>> toTransfer =
            new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    long bytesAvailableInDir = selectedDirView.getAvailableBytes();
    if (bytesAvailableInDir >= availableBytes) {
      // No need to evict anything, return an eviction plan with empty instructions.
      return new EvictionPlan(toTransfer, toEvict);
    }

    // 3. Collect victim blocks from the selected StorageDirView. They could either be evicted or
    // moved.
    List<BlockMeta> victimBlocks = new ArrayList<BlockMeta>();
    for (BlockMeta block : selectedDirView.getEvictableBlocks()) {
      victimBlocks.add(block);
      bytesAvailableInDir += block.getBlockSize();
      if (bytesAvailableInDir >= availableBytes) {
        break;
      }
    }

    // 4. Make best effort to transfer victim blocks to lower tiers rather than evict them.
    Map<StorageDirView, Long> pendingBytesInDir = new HashMap<StorageDirView, Long>();
    for (BlockMeta block : victimBlocks) {
      // TODO: should avoid calling getParentDir
      int fromTierAlias = block.getParentDir().getParentTier().getTierAlias();
      List<StorageTierView> toTiers = mManagerView.getTierViewsBelow(fromTierAlias);
      StorageDirView toDir = selectDirToTransferBlock(block, toTiers, pendingBytesInDir);
      if (toDir == null) {
        // Not possible to transfer
        toEvict.add(block.getBlockId());
      } else {
        StorageTierView toTier = toDir.getParentTierView();
        toTransfer.add(new Pair<Long, BlockStoreLocation>(block.getBlockId(),
                new BlockStoreLocation(toTier.getTierViewAlias(), toTier.getTierViewLevel(), toDir
                        .getDirViewIndex())));
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytesInDir.put(toDir, pendingBytesInDir.get(toDir) + block.getBlockSize());
        } else {
          pendingBytesInDir.put(toDir, block.getBlockSize());
        }
      }
    }
    return new EvictionPlan(toTransfer, toEvict);
  }

  // TODO: share this as a util function as it may be useful for other Evictors.
  private boolean canEvictBlocksFromDir(StorageDirView dirView, long availableBytes) {
    return dirView.getAvailableBytes() + dirView.getEvitableBytes() >= availableBytes;
  }

  private StorageDirView selectDirToEvictBlocksFromAnyTier(long availableBytes) {
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (canEvictBlocksFromDir(dirView, availableBytes)) {
          return dirView;
        }
      }
    }
    return null;
  }

  private StorageDirView selectDirToEvictBlocksFromTier(StorageTierView tierView,
                                                        long availableBytes) {
    for (StorageDirView dirView : tierView.getDirViews()) {
      if (canEvictBlocksFromDir(dirView, availableBytes)) {
        return dirView;
      }
    }
    return null;
  }

  private StorageDirView selectDirToTransferBlock(BlockMeta block, List<StorageTierView> toTiers,
                                                  Map<StorageDirView, Long> pendingBytesInDir) {
    for (StorageTierView toTier : toTiers) {
      for (StorageDirView toDir : toTier.getDirViews()) {
        long pendingBytes = 0;
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytes = pendingBytesInDir.get(toDir);
        }
        if (toDir.getAvailableBytes() - pendingBytes >= block.getBlockSize()) {
          return toDir;
        }
      }
    }
    return null;
  }

  /*
  private EvictionPlan freeSpace(long availableBytes, BlockStoreLocation location)
          throws IOException {
    getLocalBlocksInfos();

    // 1. Select a StorageDirView that has enough capacity for required bytes.
    StorageDirView selectedDirView = null;
    Set<Pair<BlockMeta, Double>> victimBlocks = new HashSet<Pair<BlockMeta, Double>>();
    List<PartitionInfoByStorage> reuseVictims = new ArrayList<PartitionInfoByStorage>();
    List<BlockMeta> ordinaryVictims = new ArrayList<BlockMeta>();
    if (location.equals(BlockStoreLocation.anyTier())) {
      selectedDirView = selectDirToEvictBlocksFromAnyTier(availableBytes, Double.MAX_VALUE,
              victimBlocks);
    } else {
      int tierAlias = location.tierAlias();
      StorageTierView tierView = mManagerView.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        selectedDirView = selectDirToEvictBlocksFromTier(tierView, availableBytes,
                Double.MAX_VALUE, victimBlocks);
      } else {
        int dirIndex = location.dir();
        StorageDirView dir = tierView.getDirView(dirIndex);
        if (canEvictBlocksFromDir(dir, availableBytes, Double.MAX_VALUE, reuseVictims, ordinaryVictims)) {
          selectedDirView = dir;
        }
      }
    }
    if (selectedDirView == null) {
      LOG.error("Failed to freeSpace: No StorageDirView has enough capacity of {} bytes",
                availableBytes);
      return null;
    }

    // 2. Check if the selected StorageDirView already has enough space.
    List<Pair<Long, BlockStoreLocation>> toTransfer =
            new java.util.ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    long bytesAvailableInDir = selectedDirView.getAvailableBytes();
    if (bytesAvailableInDir >= availableBytes) {
          // No need to evict anything, return an eviction plan with empty instructions.
      return new EvictionPlan(toTransfer, toEvict);
    }

      // 4. Make best effort to transfer victim blocks to lower tiers rather than evict them.
    Map<StorageDirView, Long> pendingBytesInDir = new HashMap<StorageDirView, Long>();
    //transferBlocks(victimBlocks, toEvict, toTransfer, pendingBytesInDir);
    transferBlocks(reuseVictims, ordinaryVictims, toEvict, toTransfer, pendingBytesInDir);
    return new EvictionPlan(toTransfer, toEvict);
  }
  */

  /*
  void transferBlocks(Set<Pair<BlockMeta, Double>> victimBlocks, List<Long> toEvict,
      List<Pair<Long, BlockStoreLocation>> toTransfer,
      Map<StorageDirView, Long> pendingBytesInDir) throws IOException {
    if (victimBlocks.isEmpty()) {
      return;
    }
    Set<Pair<BlockMeta, Double>> nextVictims = new HashSet<Pair<BlockMeta, Double>>();
    for (Pair<BlockMeta, Double> blockInfo : victimBlocks) {
          // TODO: should avoid calling getParentDir
      BlockMeta block = blockInfo.getFirst();
      int fromTierAlias = block.getParentDir().getParentTier().getTierAlias();
      List<StorageTierView> toTiers = mManagerView.getTierViewsBelow(fromTierAlias);
      //无级连替换，下一层没有足够空间，直接evict，否则transfer
      StorageDirView toDir = selectDirToTransferBlock(block, blockInfo.getSecond(), toTiers,
          pendingBytesInDir, nextVictims);
      if (toDir == null) {
        // Not possible to transfer
        toEvict.add(block.getBlockId());
      } else {
        StorageTierView toTier = toDir.getParentTierView();
        toTransfer.add(new Pair<Long, BlockStoreLocation>(block.getBlockId(),
            new BlockStoreLocation(toTier.getTierViewAlias(), toTier.getTierViewLevel(),
                  toDir.getDirViewIndex())));
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytesInDir.put(toDir, pendingBytesInDir.get(toDir) + block.getBlockSize());
        } else {
          pendingBytesInDir.put(toDir, block.getBlockSize());
        }
      }
    }
    transferBlocks(nextVictims, toEvict, toTransfer, pendingBytesInDir);
  }
  */

  /*
  boolean canEvictBlocksFromDir(StorageDirView dirView, long availableBytes, double benefit,
                                Set<Pair<BlockMeta, Double>> victims) {
    //Set<PartitionInfo> blocks = mBlockInfos.get(
    //        new Pair<Integer, Integer>(dirView.getParentTierView().getTierViewLevel(),
    //                dirView.getDirViewIndex()));

    long requiredBytes = availableBytes - dirView.getAvailableBytes();
    List<BlockMeta> allBlocks = dirView.getEvictableBlocks();
    Pair<Integer, Integer> storageIndex =
            new Pair<Integer, Integer>(dirView.getParentTierView().getTierViewLevel(),
            dirView.getDirViewIndex());
    for (PartitionInfoByStorage storage : partitionsByStorage) {
      if (requiredBytes <= 0) {
        return true;
      }

      if (storage.getBenefit() > benefit) {
        return false;
      }

      boolean evictPartition = false;
      Set<Long> blocks = storage.getBlockIds().get(storageIndex);
      for (long blockId : blocks) {
        if (mManagerView.isBlockEvictable(blockId)) {
          BlockMeta meta = null;
          try {
            meta = mManagerView.getBlockMeta(blockId);
          } catch (IOException e) {
            LOG.error(String.format("Block %d not exists on local worker!", blockId));
          }
          if (meta != null) {
            requiredBytes -= meta.getBlockSize();
            victims.add(new Pair<BlockMeta, Double>(meta, storage.getBenefit()));
            allBlocks.remove(blockId);
            evictPartition = true;
          }
        }
      }

      if (evictPartition) { //remove other blocks of the partition that belongs to different storage position
        for (Map.Entry<Pair<Integer, Integer>, Set<Long>> entry : storage.getBlockIds().entrySet()) {
          if (entry.getKey().equals(storageIndex)) {
            continue;
          }
          for (long blockId : entry.getValue()) {
            if (mManagerView.isBlockEvictable(blockId)) {
              BlockMeta meta = null;
              try {
                meta = mManagerView.getBlockMeta(blockId);
              } catch (IOException e) {
                LOG.error(String.format("Block %d not exists on local worker!", blockId));
              }
              if (meta != null) {
                victims.add(new Pair<BlockMeta, Double>(meta, storage.getBenefit()));
                allBlocks.remove(blockId);
              }
            }
          }
        }
      }
    }

    //evict other blocks that doesn't belong to our reuse storage
    for (BlockMeta block : allBlocks) {
      requiredBytes -= block.getBlockSize();
      victims.add(new Pair<BlockMeta, Double>(block, Double.MAX_VALUE));
    }
    if (requiredBytes > 0) {
      victims.clear();
      return false;
    } else {
      return true;
    }
    //false  victims is empty
    //true  victims can either be empty or not empty
  }
  */

  /*
  boolean canEvictBlocksFromDir(StorageDirView dirView, long availableBytes, double benefit,
                                Map<StorageDirView, Long> pendingBytesInDir,
                                Set<Pair<BlockMeta, Double>> victims) {
    long pendingBytes = 0L;
    if (pendingBytesInDir.containsKey(dirView)) {
      pendingBytes = pendingBytesInDir.get(dirView);
    }
    availableBytes += pendingBytes;
    return canEvictBlocksFromDir(dirView, availableBytes, benefit, victims);
  }
  */

  /*
  private StorageDirView selectDirToEvictBlocksFromAnyTier(long availableBytes, double benefit,
      Set<Pair<BlockMeta, Double>> victims) {
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= availableBytes) {
          return dirView;
        }
        if (canEvictBlocksFromDir(dirView, availableBytes, benefit, victims)) {
          return dirView;
        }
      }
    }
    return null;
  }
  */

  /*
  private StorageDirView selectDirToEvictBlocksFromTier(StorageTierView tierView,
      long availableBytes, double benefit, Set<Pair<BlockMeta, Double>> victims) {
    for (StorageDirView dirView : tierView.getDirViews()) {
      if (dirView.getAvailableBytes() >= availableBytes) {
        return dirView;
      }
      if (canEvictBlocksFromDir(dirView, availableBytes, benefit, victims)) {
        return dirView;
      }
    }
    return null;
  }
  */

  /*
  private StorageDirView selectDirToTransferBlock(BlockMeta blockInfo, double benefit,
      List<StorageTierView> toTiers, Map<StorageDirView, Long> pendingBytesInDir,
      Set<Pair<BlockMeta, Double>> victims) {
    for (StorageTierView toTier : toTiers) {
      for (StorageDirView toDir: toTier.getDirViews()) {
        long pendingBytes = 0L;
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytes = pendingBytesInDir.get(toDir);
        }
        if (toDir.getAvailableBytes() - pendingBytes >= blockInfo.getBlockSize()) {
          return toDir;
        }
        if (canEvictBlocksFromDir(toDir, blockInfo.getBlockSize(), benefit,
                pendingBytesInDir, victims)) {
          return toDir;
        }
      }
    }
    return null;
  }
  */
}

