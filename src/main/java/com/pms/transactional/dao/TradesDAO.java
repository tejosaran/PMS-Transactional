package com.pms.transactional.dao;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

import com.pms.transactional.entities.TradesEntity;

public interface TradesDAO extends JpaRepository<TradesEntity, UUID> {

}
