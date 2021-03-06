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

package tachyon.worker.block.allocator;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.thrift.PartitionInfo;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * An allocator that allocates a block in the storage dir with most free space.
 * It always allocates to the highest tier if the requested block store location is any tier.
 */
public class MaxFreeAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  public MaxFreeAllocator(BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view);
  }

  @Override
  public TempBlockMeta allocateBlockWithView(long userId, long blockId, long blockSize,
      BlockStoreLocation location, BlockMetadataManagerView view) throws IOException {
    mManagerView = view;
    return allocateBlock(userId, blockId, blockSize, location);
  }

  /**
   * Should only be accessed by {@link allocateBlockWithView} inside class.
   * Allocates a block from the given block store location. The location can be a specific location,
   * or {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(int)}.
   *
   * @param userId the ID of user to apply for the block allocation
   * @param blockId the ID of the block
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @return a temp block meta if success, null otherwise
   * @throws IOException if block location is invalid
   */
  private TempBlockMeta allocateBlock(long userId, long blockId, long blockSize,
      BlockStoreLocation location) throws IOException {
    StorageDirView candidateDirView = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        candidateDirView = getCandidateDirInTier(tierView, blockSize);
        if (candidateDirView != null) {
          return candidateDirView.createTempBlockMeta(userId, blockId, blockSize);
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      candidateDirView = getCandidateDirInTier(tierView, blockSize);
    } else {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      StorageDirView dirView = tierView.getDirView(location.dir());
      if (dirView.getAvailableBytes() >= blockSize) {
        candidateDirView = dirView;
      }
    }

    return candidateDirView != null
        ? candidateDirView.createTempBlockMeta(userId, blockId, blockSize) : null;
  }

  /**
   * Find a directory view in a tier view that has max free space and is able to store the block.
   *
   * @param tierView the storage tier view
   * @param blockSize the size of block in bytes
   * @return the storage directory view if found, null otherwise
   */
  private StorageDirView getCandidateDirInTier(StorageTierView tierView, long blockSize) {
    StorageDirView candidateDirView = null;
    long maxFreeBytes = blockSize - 1;
    for (StorageDirView dirView : tierView.getDirViews()) {
      if (dirView.getAvailableBytes() > maxFreeBytes) {
        maxFreeBytes = dirView.getAvailableBytes();
        candidateDirView = dirView;
      }
    }
    return candidateDirView;
  }

  @Override
  public StorageDirView allocatePartitionWithView(long partitionSize, List<BlockStoreLocation> locations) throws IOException {
    StorageDirView candidateDirView = null;
    for (BlockStoreLocation location : locations) {
      if (location.equals(BlockStoreLocation.anyTier())) {
        for (StorageTierView tierView : mManagerView.getTierViews()) {
          candidateDirView = getCandidateDirInTier(tierView, partitionSize);
          if (candidateDirView != null) {
            return candidateDirView;
          }
        }
      } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
        StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
        candidateDirView = getCandidateDirInTier(tierView, partitionSize);
      } else {
        StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
        StorageDirView dirView = tierView.getDirView(location.dir());
        if (dirView.getAvailableBytes() >= partitionSize) {
          candidateDirView = dirView;
        }
      }
      if (candidateDirView != null) {
        return candidateDirView;
      }
    }

    return candidateDirView;
  }
}
