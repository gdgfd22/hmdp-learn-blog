let commonURL = "/api";

const ACCESS_TOKEN_KEY = "accessToken";
const REFRESH_TOKEN_KEY = "refreshToken";
const TOKEN_KEY = "token";
const TOKEN_TYPE_KEY = "tokenType";
const EXPIRES_IN_KEY = "accessTokenExpiresIn";

axios.defaults.baseURL = commonURL;
axios.defaults.timeout = 2000;

const authClient = axios.create({
  baseURL: commonURL,
  timeout: 2000
});

let refreshPromise = null;

function getAccessToken() {
  return sessionStorage.getItem(ACCESS_TOKEN_KEY) || sessionStorage.getItem(TOKEN_KEY);
}

function getRefreshToken() {
  return sessionStorage.getItem(REFRESH_TOKEN_KEY);
}

function setAccessToken(token) {
  if (!token) {
    return;
  }
  sessionStorage.setItem(ACCESS_TOKEN_KEY, token);
  sessionStorage.setItem(TOKEN_KEY, token);
}

function saveAuthFromResponse(payload, headers) {
  if (payload) {
    setAccessToken(payload);
  }
  if (!headers) {
    return;
  }
  const refreshToken = headers["refresh-token"];
  const tokenType = headers["token-type"];
  const expiresIn = headers["access-token-expires-in"];
  if (refreshToken) {
    sessionStorage.setItem(REFRESH_TOKEN_KEY, refreshToken);
  }
  if (tokenType) {
    sessionStorage.setItem(TOKEN_TYPE_KEY, tokenType);
  }
  if (expiresIn) {
    sessionStorage.setItem(EXPIRES_IN_KEY, expiresIn);
  }
}

function clearAuth() {
  sessionStorage.removeItem(ACCESS_TOKEN_KEY);
  sessionStorage.removeItem(REFRESH_TOKEN_KEY);
  sessionStorage.removeItem(TOKEN_KEY);
  sessionStorage.removeItem(TOKEN_TYPE_KEY);
  sessionStorage.removeItem(EXPIRES_IN_KEY);
}

function goLogin() {
  if (location.pathname !== "/login.html") {
    location.href = "/login.html";
  }
}

function hasAuth() {
  return !!getAccessToken();
}

function attachAuthHeaders(config) {
  config.headers = config.headers || {};
  const accessToken = getAccessToken();
  const refreshToken = getRefreshToken();
  if (accessToken) {
    config.headers["authorization"] = accessToken;
  }
  if (refreshToken) {
    config.headers["refresh-token"] = refreshToken;
  }
  return config;
}

function refreshAccessToken() {
  const refreshToken = getRefreshToken();
  if (!refreshToken) {
    return Promise.reject(new Error("No refresh token"));
  }
  if (!refreshPromise) {
    refreshPromise = authClient.post("/user/refresh", null, {
      headers: {
        "refresh-token": refreshToken
      }
    }).then((response) => {
      if (!response.data.success) {
        return Promise.reject(new Error(response.data.errorMsg || "Refresh failed"));
      }
      saveAuthFromResponse(response.data.data, response.headers);
      return getAccessToken();
    }).finally(() => {
      refreshPromise = null;
    });
  }
  return refreshPromise;
}

axios.interceptors.request.use(
  (config) => attachAuthHeaders(config),
  (error) => Promise.reject(error)
);

axios.interceptors.response.use(
  (response) => {
    if (!response.data.success) {
      return Promise.reject(response.data.errorMsg);
    }
    return {
      data: response.data.data,
      headers: response.headers,
      status: response.status,
      raw: response.data
    };
  },
  (error) => {
    const response = error.response;
    const originalRequest = error.config || {};
    if (!response) {
      return Promise.reject("Server unavailable");
    }
    if (response.status === 401 && !originalRequest._retry && originalRequest.url !== "/user/refresh") {
      originalRequest._retry = true;
      return refreshAccessToken()
        .then(() => {
          originalRequest.headers = originalRequest.headers || {};
          originalRequest.headers["authorization"] = getAccessToken();
          originalRequest.headers["refresh-token"] = getRefreshToken();
          return axios(originalRequest);
        })
        .catch(() => {
          clearAuth();
          goLogin();
          return Promise.reject("Please login first");
        });
    }
    if (response.status === 401) {
      clearAuth();
      goLogin();
      return Promise.reject("Please login first");
    }
    return Promise.reject("Server error");
  }
);

axios.defaults.paramsSerializer = function(params) {
  let p = "";
  Object.keys(params).forEach((k) => {
    if (params[k] !== undefined && params[k] !== null && params[k] !== "") {
      p = p + "&" + k + "=" + encodeURIComponent(params[k]);
    }
  });
  return p;
}

const util = {
  commonURL,
  saveAuthFromResponse,
  clearAuth,
  hasAuth,
  getAccessToken,
  getRefreshToken,
  getUrlParam(name) {
    let reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)", "i");
    let r = window.location.search.substr(1).match(reg);
    if (r != null) {
      return decodeURI(r[2]);
    }
    return "";
  },
  formatDateTime(value) {
    if (!value) {
      return "";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return value;
    }
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    const hh = String(date.getHours()).padStart(2, "0");
    const mm = String(date.getMinutes()).padStart(2, "0");
    return `${y}-${m}-${d} ${hh}:${mm}`;
  },
  formatPrice(val) {
    if (typeof val === "string") {
      if (isNaN(val)) {
        return null;
      }
      const index = val.lastIndexOf(".");
      let p = "";
      if (index < 0) {
        p = val + "00";
      } else if (index === val.length - 2) {
        p = val.replace(".", "") + "0";
      } else {
        p = val.replace(".", "");
      }
      return parseInt(p, 10);
    } else if (typeof val === "number") {
      if (!val) {
        return null;
      }
      const s = val + "";
      if (s.length === 0) {
        return "0.00";
      }
      if (s.length === 1) {
        return "0.0" + val;
      }
      if (s.length === 2) {
        return "0." + val;
      }
      const i = s.indexOf(".");
      if (i < 0) {
        return s.substring(0, s.length - 2) + "." + s.substring(s.length - 2);
      }
      const num = s.substring(0, i) + s.substring(i + 1);
      if (i === 1) {
        return "0.0" + num;
      }
      if (i === 2) {
        return "0." + num;
      }
      if (i > 2) {
        return num.substring(0, i - 2) + "." + num.substring(i - 2);
      }
    }
    return null;
  }
}