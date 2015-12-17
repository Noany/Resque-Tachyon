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

package tachyon.worker.block.meta;

import tachyon.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by zengdan on 15-11-12.
 */
public class PartitionInfoByStorage {
    private int mId;
    private double mBenefit;
    private int tierLevel;   //blocks in the same partition should be in the same tierLevel or dirIndex
    private int dirIndex;
    //private Map<Pair<Integer, Integer>, Set<Long>> mBlockIds;
    private Set<Long> mBlockIds;

    public PartitionInfoByStorage(int id, double benefit, Set<Long> metas) {
        this.mId = id;
        this.mBenefit = benefit;
        this.mBlockIds = metas;
    }

    public PartitionInfoByStorage(int id, double benefit) {
        this.mId = id;
        this.mBenefit = benefit;
        this.mBlockIds = new HashSet<Long>();
    }

    public void addBlockMeta(long id) {
        //Pair<Integer, Integer> key = new Pair<Integer, Integer>(tierLevel, dirIndex);
        //if (mBlockIds.get(dirIndex) == null) {
        //    mBlockIds.put(dirIndex, new HashSet<Long>());
        //}
        //mBlockIds.get(dirIndex).add(id);
        mBlockIds.add(id);
    }

    public Set<Long> getBlockIds() {
        return mBlockIds;
    }

    public int getId() {
        return mId;
    }

    public double getBenefit() {
        return mBenefit;
    }
}
