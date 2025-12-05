package com.pms.transactional.dao;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.pms.transactional.entities.TradesEntity;

@Repository
public interface TradesDao extends JpaRepository<TradesEntity, UUID> {

}
