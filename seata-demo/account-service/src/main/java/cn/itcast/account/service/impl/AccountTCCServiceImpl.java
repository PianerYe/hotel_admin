package cn.itcast.account.service.impl;

import cn.itcast.account.entity.AccountFreeze;
import cn.itcast.account.mapper.AccountFreezeMapper;
import cn.itcast.account.mapper.AccountMapper;
import cn.itcast.account.service.AccountTCCService;
import io.seata.core.context.RootContext;
import io.seata.rm.tcc.api.BusinessActionContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

@Slf4j
@Service
public class AccountTCCServiceImpl implements AccountTCCService {

    @Resource
    private AccountMapper accountMapper;
    @Resource
    private AccountFreezeMapper freezeMapper;

    @Override
    @Transactional
    public void deduct(String userId, int money) {
        //获取事务ID
        String xid = RootContext.getXID();
        //判断freeze中是否有冻结记录，如果有，一定是cancel执行过了，我要拒绝
        AccountFreeze oldFreeze = freezeMapper.selectById(xid);
        if (oldFreeze != null){
            // CANCEL执行过，拒接业务
            return;
        }
        // 1.扣减可用余额
        accountMapper.deduct(userId,money);
        // 2.记录冻结金额，事务状态
        AccountFreeze freeze = new AccountFreeze();
        freeze.setUserId(userId);
        freeze.setFreezeMoney(money);
        freeze.setState(AccountFreeze.State.TRY);
        freeze.setXid(xid);
        freezeMapper.insert(freeze);
    }

    @Override
    public boolean confirm(BusinessActionContext ctx) {
        //获取事务ID
        String xid = ctx.getXid();
        // 2.根据id删除冻结记录
        int count = freezeMapper.deleteById(xid);
        return count == 1;
    }

    @Override
    public boolean cancel(BusinessActionContext ctx) {
        //查询冻结记录
        String xid = ctx.getXid();
        String userId = ctx.getActionContext("userId").toString();
        AccountFreeze freeze = freezeMapper.selectById(xid);
        //空回滚判读，判断freeze是否为null，为null证明try没执行，需要空回滚
        if(freeze == null){
            //证明try没有执行，需要空回滚
            freeze = new AccountFreeze();
            freeze.setUserId(userId);
            freeze.setFreezeMoney(0);
            freeze.setState(AccountFreeze.State.CONFIRM);
            freeze.setXid(xid);
            freezeMapper.insert(freeze);
        }
        //判断幂等
        if (freeze.getState() == AccountFreeze.State.CANCEL){
            //已经处理过一次cancel了，无需重复处理
            return true;
        }
        //1.恢复可用余额
        accountMapper.refund(freeze.getUserId(),freeze.getFreezeMoney());
        //2.冻结金额清零，状态改为cancel
        freeze.setFreezeMoney(0);
        freeze.setState(AccountFreeze.State.CANCEL);
        int account = freezeMapper.updateById(freeze);
        return account == 1;
    }
}
