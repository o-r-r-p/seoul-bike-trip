FROM kaggle/python

# 作業ディレクトリの設定
WORKDIR /app

# ポートの開放
EXPOSE 8888

# JupyterLabを起動
# CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--notebook-dir=/app/notebooks"]
