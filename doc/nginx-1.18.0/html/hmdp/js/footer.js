Vue.component("foot-bar", {
  template: `
    <div class="foot">
      <div class="foot-box" :class="{active: activeBtn === 1}" @click="toPage(1)">
        <div class="foot-view"><i class="el-icon-s-home"></i></div>
        <div class="foot-text">Home</div>
      </div>
      <div class="foot-box" :class="{active: activeBtn === 2}" @click="toPage(2)">
        <div class="foot-view"><i class="el-icon-map-location"></i></div>
        <div class="foot-text">Map</div>
      </div>
      <div class="foot-box" @click="toPage(0)">
        <img class="add-btn" src="/imgs/add.png" alt="">
      </div>
      <div class="foot-box" :class="{active: activeBtn === 3}" @click="toPage(3)">
        <div class="foot-view foot-view-badge">
          <i class="el-icon-chat-dot-round"></i>
          <span v-if="unreadCount > 0" class="foot-badge">{{badgeText}}</span>
        </div>
        <div class="foot-text">Notice</div>
      </div>
      <div class="foot-box" :class="{active: activeBtn === 4}" @click="toPage(4)">
        <div class="foot-view"><i class="el-icon-user"></i></div>
        <div class="foot-text">Me</div>
      </div>
    </div>
  `,
  props: ["activeBtn"],
  data() {
    return {
      unreadCount: 0
    };
  },
  computed: {
    badgeText() {
      return this.unreadCount > 99 ? "99+" : this.unreadCount;
    }
  },
  created() {
    const token = sessionStorage.getItem('accessToken') || sessionStorage.getItem('token');
    if (token) this.loadUnreadCount();
  },
  methods: {
    loadUnreadCount() {
      axios.get("/notification/unread/count")
          .then(({data}) => { this.unreadCount = Number(data) || 0; })
          .catch(() => { this.unreadCount = 0; });
    },
    toPage(i) {
      switch(i) {
        case 0: location.href = "/blog-edit.html"; break;
        case 1: location.href = "/"; break;
        case 3: location.href = "/notification.html"; break;
        case 4: location.href = "/info.html"; break;
      }
    }
  }
});