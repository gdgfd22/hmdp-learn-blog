import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';

const demoDashboard = {
  overview: [{ dau: 12842, shop_visit_pv: 48632, order_count: 3268, paid_order_count: 2987, gmv: 28645000, seckill_success_rate: 73.6, like_count: 8421, unlike_count: 326, follow_count: 1946, unfollow_count: 183 }],
  shopRank: [
    { shop_id: 118, shop_name: '山野小馆', category: '江浙菜', hot_score: 9638, visit_uv: 4260, order_count: 582 },
    { shop_id: 52, shop_name: '木棉咖啡', category: '咖啡', hot_score: 8245, visit_uv: 3891, order_count: 436 },
    { shop_id: 206, shop_name: '湖畔面包房', category: '烘焙', hot_score: 7014, visit_uv: 3218, order_count: 389 },
    { shop_id: 87, shop_name: '青藤小酒馆', category: '酒馆', hot_score: 6180, visit_uv: 2865, order_count: 312 },
    { shop_id: 143, shop_name: '晚风食堂', category: '创意菜', hot_score: 5326, visit_uv: 2476, order_count: 268 }
  ],
  blogRank: [{blog_id:101, title:'西湖边不挤的晚霞位置', hot_score:7632},{blog_id:102,title:'梧桐树下的面包房',hot_score:6480},{blog_id:103,title:'周末花市散步地图',hot_score:5210}],
  voucherFunnel: [{voucher_id:32, voucher_name:'周末双人餐',exposure_count:18520,seckill_request_count:8230,order_count:4680,paid_order_count:4206,pay_rate:90.0},{voucher_id:18,voucher_name:'咖啡买一赠一',exposure_count:14380,seckill_request_count:6910,order_count:3528,paid_order_count:3196,pay_rate:90.6},{voucher_id:46,voucher_name:'城市夜游券',exposure_count:9680,seckill_request_count:4210,order_count:1980,paid_order_count:1702,pay_rate:86.0}],
  quality: [{check_name:'MySQL → Doris 订单对账',error_count:0,sample_message:'最近一次检查 21:30 · 数据一致'},{check_name:'行为事件延迟监控',error_count:2,sample_message:'2 条事件延迟超过 30 秒'},{check_name:'优惠券重复消费检查',error_count:0,sample_message:'未发现重复消费'}]
};

const trend = [
  {time:'08:00', visits:640, orders:126},{time:'10:00',visits:1180,orders:248},{time:'12:00',visits:2460,orders:520},{time:'14:00',visits:1980,orders:436},{time:'16:00',visits:2840,orders:618},{time:'18:00',visits:3760,orders:824},{time:'20:00',visits:4380,orders:968},{time:'22:00',visits:3260,orders:742}
];

const format = value => Number(value || 0).toLocaleString('zh-CN');
const money = value => (Number(value || 0) / 100).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function Sparkline({ data, accessor, color }) {
  const width = 720, height = 215, pad = 14;
  const values = data.map(accessor), max = Math.max(...values, 1);
  const points = values.map((value, index) => `${pad + index * (width-pad*2)/(values.length-1)},${height-pad-value*(height-pad*2)/max}`).join(' ');
  const area = `${pad},${height-pad} ${points} ${width-pad},${height-pad}`;
  return <svg className="trend-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="分时趋势图"><defs><linearGradient id={`fade-${color.replace('#','')}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0" stopColor={color} stopOpacity=".28"/><stop offset="1" stopColor={color} stopOpacity="0"/></linearGradient></defs><g className="grid-lines">{[40,85,130,175].map(y=><line key={y} x1="0" y1={y} x2={width} y2={y}/>)}</g><polygon points={area} fill={`url(#fade-${color.replace('#','')})`}/><polyline points={points} fill="none" stroke={color} strokeWidth="4" strokeLinecap="round" strokeLinejoin="round"/>{values.map((value,index)=>{const [x,y]=points.split(' ')[index].split(','); return <circle key={index} cx={x} cy={y} r="4" fill="#fff" stroke={color} strokeWidth="3"/>;})}</svg>;
}

function MetricCard({ label, value, note, tone='green', delta }) {
  return <article className={`metric-card ${tone}`}><div><span>{label}</span>{delta && <small className="delta">↗ {delta}</small>}</div><strong>{value}</strong><p>{note}</p></article>;
}

function AnalyticsApp() {
  const [data, setData] = useState(demoDashboard);
  const [mode, setMode] = useState('demo');
  const [date, setDate] = useState(new Date().toISOString().slice(0,10));
  const [period, setPeriod] = useState('今日');
  const overview = data.overview?.[0] || {};
  const maxScore = useMemo(() => Math.max(...(data.shopRank || []).map(x => Number(x.hot_score)),1), [data]);

  const load = () => {
    fetch(`/analytics/dashboard?date=${date}`).then(response => response.ok ? response.json() : Promise.reject()).then(payload => {
      const body = payload.data || payload;
      if (body?.overview?.length) { setData(body); setMode('live'); }
    }).catch(() => { setData(demoDashboard); setMode('demo'); });
  };
  useEffect(load, []);

  return <div className="analytics-app">
    <aside className="side-nav"><a className="analytics-brand" href="./index.html"><span>城</span><b>城事<br/>数据</b></a><nav><a className="active" href="#overview"><i>⌁</i><span>总览</span></a><a href="#trend"><i>⌇</i><span>趋势</span></a><a href="#shops"><i>◇</i><span>商户</span></a><a href="#funnel"><i>▽</i><span>转化</span></a><a href="#quality"><i>✓</i><span>质量</span></a></nav><a className="back-home" href="./index.html">←<span>返回首页</span></a></aside>
    <main className="dashboard-main">
      <header className="dashboard-header"><div><p className="eyebrow">CITY PULSE · REALTIME ANALYTICS</p><h1>城市脉搏</h1><p>从行为事件到订单成交，读懂今天正在发生的消费选择。</p></div><div className="header-tools"><span className={`data-status ${mode}`}><i/> {mode === 'live' ? '实时数据' : '演示数据'}</span><label><span>日期</span><input type="date" value={date} onChange={e=>setDate(e.target.value)}/></label><button onClick={load}>↻ 刷新</button></div></header>

      <section id="overview" className="metric-grid"><MetricCard label="今日活跃用户" value={format(overview.dau)} note="去重活跃用户 DAU" delta="12.6%"/><MetricCard label="商户访问" value={format(overview.shop_visit_pv)} note="详情页累计浏览" delta="8.4%" tone="blue"/><MetricCard label="成交订单" value={format(overview.paid_order_count)} note={`下单 ${format(overview.order_count)} 笔`} delta="16.2%" tone="orange"/><MetricCard label="实时交易额" value={`¥ ${money(overview.gmv)}`} note="已支付订单 GMV" delta="21.8%" tone="gold"/></section>

      <section className="dashboard-grid" id="trend">
        <article className="panel trend-panel"><header className="panel-header"><div><span className="panel-index">01</span><div><h2>城市活跃趋势</h2><p>访问与成交的小时级变化</p></div></div><div className="segmented">{['今日','近 7 日','近 30 日'].map(item=><button key={item} className={period===item?'active':''} onClick={()=>setPeriod(item)}>{item}</button>)}</div></header><div className="chart-legend"><span><i className="visit-dot"/>访问用户</span><span><i className="order-dot"/>成交订单</span><b>峰值 20:00</b></div><div className="chart-stack"><Sparkline data={trend} accessor={x=>x.visits} color="#17624a"/><Sparkline data={trend} accessor={x=>x.orders*4} color="#e26b48"/></div><div className="x-axis">{trend.map(x=><span key={x.time}>{x.time}</span>)}</div></article>

        <article className="panel pulse-panel"><header className="panel-header"><div><span className="panel-index">02</span><div><h2>互动温度</h2><p>用户关系与内容反馈</p></div></div></header><div className="pulse-score"><div className="score-ring"><span><strong>86</strong><small>热度指数</small></span></div><p><b>城市互动保持活跃</b><span>较昨日同一时段提升 9.8%</span></p></div><div className="interaction-list"><span><i>赞</i><b>{format(overview.like_count)}</b><small>点赞</small></span><span><i>关</i><b>{format(overview.follow_count)}</b><small>新增关注</small></span><span><i>消</i><b>{format(overview.unlike_count)}</b><small>取消点赞</small></span></div></article>

        <article className="panel shop-panel" id="shops"><header className="panel-header"><div><span className="panel-index">03</span><div><h2>商户热力排行</h2><p>UV + 净点赞 × 3 + 订单 × 5</p></div></div><a href="#shops">查看全部 ↗</a></header><div className="shop-table"><div className="shop-table-head"><span>排名 / 商户</span><span>分类</span><span>热度变化</span><span>访问 UV</span><span>成交</span><span>热度分</span></div>{data.shopRank.map((shop,index)=><div className="shop-row" key={shop.shop_id}><span className="shop-name"><i>{String(index+1).padStart(2,'0')}</i><b>{shop.shop_name || `商户 #${shop.shop_id}`}</b></span><span><em>{shop.category || '本地生活'}</em></span><span className="mini-bar"><i style={{width:`${Number(shop.hot_score)*100/maxScore}%`}}/></span><span>{format(shop.visit_uv)}</span><span>{format(shop.order_count)}</span><strong>{format(shop.hot_score)}</strong></div>)}</div></article>

        <article className="panel funnel-panel" id="funnel"><header className="panel-header"><div><span className="panel-index">04</span><div><h2>优惠券转化</h2><p>从曝光到支付的关键漏斗</p></div></div></header>{data.voucherFunnel.slice(0,3).map((item,index)=><div className="funnel-item" key={item.voucher_id}><div className="funnel-title"><span><i>{index+1}</i><b>{item.voucher_name || `优惠券 #${item.voucher_id}`}</b></span><strong>{Number(item.pay_rate||0).toFixed(1)}% <small>支付转化</small></strong></div><div className="funnel-track"><span style={{width:'100%'}}/><span style={{width:`${item.seckill_request_count*100/item.exposure_count}%`}}/><span style={{width:`${item.paid_order_count*100/item.exposure_count}%`}}/></div><div className="funnel-labels"><span>曝光 {format(item.exposure_count)}</span><span>请求 {format(item.seckill_request_count)}</span><span>支付 {format(item.paid_order_count)}</span></div></div>)}</article>

        <article className="panel quality-panel" id="quality"><header className="panel-header"><div><span className="panel-index">05</span><div><h2>数据质量哨兵</h2><p>链路一致性与延迟监控</p></div></div><span className="healthy">● 整体健康</span></header>{data.quality.slice(0,3).map(item=><div className="quality-row" key={item.check_name}><i className={item.error_count?'warning':'ok'}>{item.error_count?'!':'✓'}</i><span><b>{item.check_name}</b><small>{item.sample_message || '检查通过'}</small></span><strong className={item.error_count?'warning-text':''}>{item.error_count ? `${item.error_count} 异常` : '正常'}</strong></div>)}<footer>最近同步 · 21:32:18 <span>端到端延迟 1.8s</span></footer></article>
      </section>
      <footer className="dashboard-footer"><span>HMDP REALTIME WAREHOUSE</span><p>Kafka · Flink · Doris · Spring Boot</p><span>数据口径 v1.2</span></footer>
    </main>
  </div>;
}

createRoot(document.getElementById('root')).render(<AnalyticsApp/>);
