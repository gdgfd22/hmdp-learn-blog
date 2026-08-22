import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';

const demoTypes = [
  { id: 1, name: '认真吃饭', icon: './imgs/types/ms.png' },
  { id: 2, name: '喝杯咖啡', icon: './imgs/types/qzyl.png' },
  { id: 3, name: '松弛一下', icon: './imgs/types/spa.png' },
  { id: 4, name: '夜晚微醺', icon: './imgs/types/jiuba.png' },
  { id: 5, name: '动起来', icon: './imgs/types/jsyd.png' }
];

const demoBlogs = [
  { id: 101, title: '藏在梧桐树下的面包房，下午四点刚好出炉', tag: '咖啡', area: '拱墅 · 大兜路', liked: 1286, name: '橘子汽水', time: '32 分钟前', icon: './imgs/icons/icon1.jpg', img: './imgs/blogs/2/1/7b31a5da-4ee3-4c9e-a38a-2dbc8de0345a.jpg', summary: '黄油香从街角飘出来，玻璃窗里是今天最柔软的一小时。' },
  { id: 102, title: '西湖边不挤的看晚霞位置，我愿意再去一百次', tag: '周末散步', area: '西湖 · 茅家埠', liked: 968, name: '阿久散步中', time: '1 小时前', icon: './imgs/icons/user5-icon.png', img: './imgs/blogs/7/1/6f4c2442-1d3b-436f-8d49-cc704f9f0bf8.jpg' },
  { id: 103, title: '一锅热气腾腾的江南味，是认真生活的证据', tag: '江浙菜', area: '上城 · 中山南路', liked: 742, name: '小满同学', time: '2 小时前', icon: './imgs/icons/kkjtbcr.jpg', img: './imgs/blogs/4/7/863cc302-d150-420d-a596-b16e9232a1a6.jpg' },
  { id: 104, title: '周末去逛花市，把春天提前带回家', tag: '周末散步', area: '萧山 · 花木城', liked: 635, name: '一页杭州', time: '3 小时前', icon: './imgs/icons/default-icon.png', img: './imgs/blogs/14/3/52b290eb-8b5d-403b-8373-ba0bb856d18e.jpg' }
];

function Brand({ footer = false }) {
  return <a className={`brand ${footer ? 'footer-brand' : ''}`} href="./index.html"><span className="brand-mark">城</span><span><b>城事点评</b><small>CITY NOTES</small></span></a>;
}

function HomeApp() {
  const [types, setTypes] = useState(demoTypes);
  const [blogs, setBlogs] = useState(demoBlogs);
  const [keyword, setKeyword] = useState('');
  const [filter, setFilter] = useState('全部');
  const [toast, setToast] = useState('');
  const hotTags = ['咖啡', '周末散步', '江浙菜'];

  useEffect(() => {
    fetch('/shop-type/list').then(r => r.ok ? r.json() : Promise.reject()).then(data => {
      if (Array.isArray(data) && data.length) setTypes(data.slice(0, 5).map(item => ({ ...item, icon: `./imgs/${item.icon}` })));
    }).catch(() => {});
    fetch('/blog/hot?current=1').then(r => r.ok ? r.json() : Promise.reject()).then(data => {
      if (Array.isArray(data) && data.length) setBlogs(data.slice(0, 4).map(item => ({ ...item, img: item.images?.split(',')[0], tag: item.tag || '城市漫游' })));
    }).catch(() => {});
  }, []);

  const filteredBlogs = useMemo(() => blogs.filter(blog =>
    (filter === '全部' || blog.tag === filter) && (!keyword || `${blog.title}${blog.area || ''}`.includes(keyword))
  ), [blogs, filter, keyword]);

  const notify = message => { setToast(message); window.setTimeout(() => setToast(''), 2000); };
  const search = value => { if (value !== undefined) setKeyword(value); notify((value || keyword) ? `为你找到关于“${value || keyword}”的灵感` : '输入一个想去的地方吧'); };
  const toggleLike = (event, blog) => {
    event.stopPropagation();
    setBlogs(items => items.map(item => item.id === blog.id ? { ...item, isLike: !item.isLike, liked: Number(item.liked || 0) + (item.isLike ? -1 : 1) } : item));
    if (blog.id < 100) fetch(`/blog/like/${blog.id}`, { method: 'PUT' }).catch(() => {});
  };

  return <>
    <header className="site-header"><Brand /><nav className="main-nav"><a className="active" href="#discover">发现</a><a href="#inspiration">灵感</a><a href="./analytics.html">城市数据</a></nav><div className="header-actions"><button className="location-button"><span className="pin">●</span> 杭州 <span>⌄</span></button><button className="avatar-button" onClick={() => location.href='./info.html'}>游</button></div></header>
    <main>
      <section className="hero" id="discover"><div className="hero-copy"><p className="eyebrow">WEEKEND IN HANGZHOU · 08 / 22</p><h1>把城市，<br/><em>过成自己的日常。</em></h1><p className="hero-intro">从一碗热汤到一场日落，发现附近真实、鲜活、值得抵达的好生活。</p><div className="search-shell"><span className="search-icon">⌕</span><input value={keyword} onChange={e => setKeyword(e.target.value)} onKeyDown={e => e.key === 'Enter' && search()} placeholder="搜餐厅、咖啡馆、展览或一条街"/><button onClick={() => search()}>去发现</button></div><div className="quick-links"><span>此刻热门</span>{hotTags.map(tag => <button key={tag} onClick={() => search(tag)}># {tag}</button>)}</div></div><div className="hero-visual"><figure className="hero-photo"><img src="./imgs/blogs/2/1/7b31a5da-4ee3-4c9e-a38a-2dbc8de0345a.jpg" alt="杭州城市生活"/><figcaption><span>本周编辑选择</span><b>沿着运河，吃一顿松弛的晚饭</b></figcaption></figure><div className="floating-note"><span>今日精选</span><strong>26</strong><small>个新鲜去处</small></div><div className="sun-shape"/></div></section>
      <section className="category-section"><div className="section-heading"><div><p className="eyebrow">EXPLORE BY MOOD</p><h2>今天，想怎么过？</h2></div><p>不用赶路，去做一件让今天变具体的小事。</p></div><div className="category-grid">{types.map((item,index) => <button className="category-card" key={item.id} onClick={() => location.href=`./shop-list.html?type=${item.id}&name=${encodeURIComponent(item.name)}`}><span className="category-number">0{index+1}</span><img src={item.icon} alt={item.name}/><span className="category-name">{item.name}</span><span className="category-arrow">↗</span></button>)}</div></section>
      <section className="editorial-section" id="inspiration"><div className="section-heading editorial-heading"><div><p className="eyebrow">LOCAL STORIES</p><h2>附近的人，正在认真生活</h2></div><div className="filter-tabs">{['全部',...hotTags].map(tag => <button className={filter===tag?'active':''} key={tag} onClick={() => setFilter(tag)}>{tag}</button>)}</div></div><div className="story-grid">{filteredBlogs.map((blog,index) => <article className={`story-card ${index===0?'featured-story':''}`} key={blog.id} onClick={() => blog.id < 100 && (location.href=`./blog-detail.html?id=${blog.id}`)}><div className="story-image"><img src={blog.img} alt={blog.title}/><span>{blog.tag || '城市漫游'}</span></div><div className="story-content"><p className="story-kicker">{blog.area || '杭州 · 城市生活'}</p><h3>{blog.title}</h3><p className="story-summary">{blog.summary || '在熟悉的街巷里，重新发现那些让人愿意慢下来的细节。'}</p><footer><div className="author"><img src={blog.icon || './imgs/icons/default-icon.png'} alt=""/><span><b>{blog.name || '城市漫游者'}</b><small>{blog.time || '2 小时前'}</small></span></div><button className={`like-button ${blog.isLike?'liked':''}`} onClick={e => toggleLike(e,blog)}>♡ <span>{Number(blog.liked || 0).toLocaleString()}</span></button></footer></div></article>)}</div><button className="load-more" onClick={() => notify('今天的好去处都在这里了')}>继续逛逛 ↓</button></section>
      <section className="data-invite"><div><p className="eyebrow">THE CITY IN NUMBERS</p><h2>不只推荐好去处，<br/>也读懂城市的每一次选择。</h2></div><div className="invite-stats"><span><strong>12.8k</strong><small>今日城市足迹</small></span><span><strong>86%</strong><small>真实口碑推荐</small></span></div><a href="./analytics.html">打开城市数据看板 <span>↗</span></a></section>
    </main>
    <footer className="site-footer"><Brand footer/><p>记录真实生活，也尊重每一种生活。</p><span>© 2026 HMDP LEARNING PROJECT</span></footer>{toast && <div className="toast">{toast}</div>}
  </>;
}

createRoot(document.getElementById('root')).render(<HomeApp />);
